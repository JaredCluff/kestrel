// crates/kestrel-agent/src/capabilities/ax/linux.rs
//
// Linux AX backend. Talks to AT-SPI (the GNOME/freedesktop
// accessibility bus) via the `atspi` crate, which is async and runs on
// top of zbus / D-Bus. We block on a small current-thread runtime
// inside `describe()` so the public entry point can stay synchronous
// and match the macOS / Windows shape.
//
// AT-SPI runtime requirements:
//   - `at-spi-bus-launcher` running (default on most desktop distros
//     under GNOME, KDE, Xfce, Cinnamon, MATE).
//   - The current process running under the same user session as the
//     desktop. SSH-from-headless almost never has AT-SPI.
//   - The session's accessibility bus environment variable visible to
//     the process (`AT_SPI_BUS_ADDRESS` or session bus discovery).
//
// On any failure (no bus, deny, runtime build failure, proxy call
// errors), we return `AccessibilityNode::unavailable()` (which sets
// `fallback: true`) so callers fall back to a screenshot.
//
// AUTHOR CAVEAT: This file's tree-walk recursion was written without
// the ability to runtime-verify on a Linux machine. The structural
// recursion (depth budget, per-child query, error-to-fallback
// degradation) is straightforward; the specific atspi proxy method
// names are taken from atspi 0.22 docs.rs and may need minor
// adjustment in a follow-up if the API has drifted. Any failure path
// converts to an empty children list with the node's role intact, so
// even partial API matches give callers useful output.

use kestrel_proto::AccessibilityNode;

/// Maximum recursion depth. Matches the macOS implementation so
/// callers get comparable output shapes across platforms.
const MAX_DEPTH: u8 = 5;

pub fn describe() -> AccessibilityNode {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::warn!("ax(linux): failed to build runtime: {}", e);
            return AccessibilityNode::unavailable();
        }
    };
    rt.block_on(describe_async())
}

async fn describe_async() -> AccessibilityNode {
    // Open the accessibility bus. If at-spi-bus-launcher isn't running
    // (headless / SSH / kiosk session), this errors and we fall back.
    let conn = match atspi::AccessibilityConnection::open().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("ax(linux): AT-SPI bus open failed: {}", e);
            return AccessibilityNode::unavailable();
        }
    };

    // Walk from the registry's root. atspi exposes the desktop frame
    // as the root of the application tree; from there we look for the
    // active application and recurse into it.
    //
    // The exact API path is `conn.registry().get_root()` or similar
    // depending on the atspi version. We use `root` proxy access if
    // available; on error, return a populated root marker so callers
    // know the bus IS reachable but the tree query specifically
    // failed.
    let root_proxy = match try_get_root_proxy(&conn).await {
        Some(p) => p,
        None => {
            return AccessibilityNode {
                role: "desktop".into(),
                label: Some(
                    "AT-SPI bus reachable but root proxy query failed".into(),
                ),
                value: None,
                focused: true,
                enabled: true,
                bounds: None,
                children: vec![],
                fallback: false,
            };
        }
    };

    walk(&root_proxy, MAX_DEPTH).await
}

/// Best-effort root-proxy fetch. Returns None on any error so the
/// caller can degrade to a populated-but-empty root node rather than
/// outright unavailable.
async fn try_get_root_proxy(
    conn: &atspi::AccessibilityConnection,
) -> Option<atspi::proxy::accessible::AccessibleProxy<'_>> {
    // atspi's `Accessible` proxy can be constructed against the well-
    // known root path "/org/a11y/atspi/accessible/root". This is the
    // standard AT-SPI entrypoint; every desktop session exposes it.
    let conn_inner = conn.connection();
    atspi::proxy::accessible::AccessibleProxy::builder(conn_inner)
        .destination("org.a11y.atspi.Registry")
        .ok()?
        .path("/org/a11y/atspi/accessible/root")
        .ok()?
        .build()
        .await
        .ok()
}

/// Recurse through the AT-SPI tree. Each child is its own D-Bus proxy
/// — `get_child_at_index(i)` returns an `Accessible` we wrap into a
/// fresh proxy. Errors on a child become `unavailable()` placeholders
/// in that subtree without aborting the whole walk.
async fn walk<'a>(
    elem: &atspi::proxy::accessible::AccessibleProxy<'a>,
    depth: u8,
) -> AccessibilityNode {
    let role = elem
        .get_role_name()
        .await
        .unwrap_or_else(|_| "unknown".to_string());
    let label = elem.name().await.ok().filter(|s: &String| !s.is_empty());
    let enabled = true; // AT-SPI exposes state-set; mapping to a single
                        // bool is a coarsening we accept here. A future
                        // refinement could surface the full StateSet.

    let mut children: Vec<AccessibilityNode> = vec![];
    if depth > 0 {
        if let Ok(count) = elem.child_count().await {
            for i in 0..count {
                match elem.get_child_at_index(i).await {
                    Ok(child_obj) => {
                        // child_obj is an `ObjectRef`; convert to a
                        // proxy so we can recurse.
                        let conn = elem.connection();
                        let proxy = atspi::proxy::accessible::AccessibleProxy::builder(conn)
                            .destination(child_obj.name)
                            .and_then(|b| b.path(child_obj.path))
                            .map(|b| b.build());
                        match proxy {
                            Ok(future) => match future.await {
                                Ok(child_proxy) => {
                                    let sub = Box::pin(walk(&child_proxy, depth - 1)).await;
                                    children.push(sub);
                                }
                                Err(_) => children.push(AccessibilityNode::unavailable()),
                            },
                            Err(_) => children.push(AccessibilityNode::unavailable()),
                        }
                    }
                    Err(_) => {
                        // Single child failing shouldn't abort the
                        // whole walk — leave a placeholder and
                        // continue.
                        children.push(AccessibilityNode::unavailable());
                    }
                }
            }
        }
    }

    AccessibilityNode {
        role,
        label,
        value: None,
        focused: false, // AT-SPI focused-tracking is per-StateSet; this
                        // backend doesn't surface it yet.
        enabled,
        bounds: None,
        children,
        fallback: false,
    }
}
