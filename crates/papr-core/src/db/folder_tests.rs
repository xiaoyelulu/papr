use super::*;

fn database() -> Connection {
    open(Path::new(":memory:")).unwrap()
}
fn subscribe(conn: &Connection, folder: i64, name: &str) -> i64 {
    conn.execute(
        "INSERT INTO feeds(feed_url,title,folder_id) VALUES(?1,?2,?3)",
        params![format!("https://{name}.example/rss"), name, folder],
    )
    .unwrap();
    let id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO articles(feed_id,guid,title) VALUES(?1,'one','Article')",
        [id],
    )
    .unwrap();
    id
}
#[test]
fn parent_views_refresh_and_bulk_read_include_descendants() {
    let conn = database();
    let root = create_folder(&conn, "Root").unwrap();
    let child = create_folder_in(&conn, "Child", Some(root)).unwrap();
    let leaf = create_folder_in(&conn, "Leaf", Some(child)).unwrap();
    let other = create_folder(&conn, "Other").unwrap();
    for (folder, name) in [
        (root, "root"),
        (child, "child"),
        (leaf, "leaf"),
        (other, "other"),
    ] {
        subscribe(&conn, folder, name);
    }
    assert_eq!(
        list_articles(
            &conn,
            &ArticleQuery::Folder(root),
            false,
            None,
            false,
            50,
            0
        )
        .unwrap()
        .len(),
        3
    );
    assert_eq!(feeds_to_refresh_in_folder(&conn, root).unwrap().len(), 3);
    assert_eq!(
        mark_all_read(&conn, &ArticleQuery::Folder(root), false).unwrap(),
        3
    );
    assert_eq!(count_unread(&conn).unwrap(), 1);
}
#[test]
fn folder_moves_reject_cycles_and_missing_parents() {
    let conn = database();
    let root = create_folder(&conn, "Root").unwrap();
    let child = create_folder_in(&conn, "Child", Some(root)).unwrap();
    assert!(matches!(
        move_folder(&conn, root, Some(child)),
        Err(AppError::Coded("folderCycle"))
    ));
    assert!(move_folder(&conn, child, Some(child)).is_err());
    assert!(move_folder(&conn, child, Some(9999)).is_err());
    assert!(conn
        .execute(
            "UPDATE folders SET parent_id=?2 WHERE id=?1",
            params![root, child]
        )
        .is_err());
    move_folder(&conn, child, None).unwrap();
    assert!(list_folders(&conn)
        .unwrap()
        .iter()
        .all(|folder| folder.parent_id.is_none()));
}
#[test]
fn deleting_only_parent_preserves_children_and_subscriptions() {
    let conn = database();
    let root = create_folder(&conn, "Root").unwrap();
    let child = create_folder_in(&conn, "Child", Some(root)).unwrap();
    let root_feed = subscribe(&conn, root, "root");
    let child_feed = subscribe(&conn, child, "child");
    delete_folder(&conn, root).unwrap();
    assert_eq!(list_folders(&conn).unwrap()[0].parent_id, None);
    assert_eq!(feed_folder_id(&conn, root_feed).unwrap(), None);
    assert_eq!(feed_folder_id(&conn, child_feed).unwrap(), Some(child));
    assert_eq!(list_feeds(&conn).unwrap().len(), 2);
}
#[test]
fn deleting_subtree_removes_only_its_feeds_and_articles_and_rolls_back_on_failure() {
    let conn = database();
    let root = create_folder(&conn, "Root").unwrap();
    let child = create_folder_in(&conn, "Child", Some(root)).unwrap();
    let other = create_folder(&conn, "Other").unwrap();
    subscribe(&conn, root, "root");
    subscribe(&conn, child, "child");
    subscribe(&conn, other, "other");
    conn.execute_batch("CREATE TRIGGER fail_folder_delete BEFORE DELETE ON folders WHEN old.name='Child' BEGIN SELECT RAISE(ABORT,'test failure'); END;").unwrap();
    assert!(delete_folder_with_feeds(&conn, root).is_err());
    assert_eq!(list_feeds(&conn).unwrap().len(), 3);
    conn.execute_batch("DROP TRIGGER fail_folder_delete;")
        .unwrap();
    delete_folder_with_feeds(&conn, root).unwrap();
    assert_eq!(list_folders(&conn).unwrap().len(), 1);
    assert_eq!(list_feeds(&conn).unwrap()[0].folder_id, Some(other));
    assert_eq!(count_unread(&conn).unwrap(), 1);
}
#[test]
fn hierarchy_rename_and_subtree_deletion_sync_across_devices() {
    let a = database();
    let mut b = database();
    let root = create_folder(&a, "Z parent").unwrap();
    let child = create_folder_in(&a, "A child", Some(root)).unwrap();
    subscribe(&a, child, "child");
    webdav::merge(&mut b, &webdav::records(&a).unwrap()).unwrap();
    let folders = list_folders(&b).unwrap();
    let parent = folders.iter().find(|f| f.name == "Z parent").unwrap().id;
    assert_eq!(
        folders
            .iter()
            .find(|f| f.name == "A child")
            .unwrap()
            .parent_id,
        Some(parent)
    );
    rename_folder(&a, root, "Renamed").unwrap();
    webdav::merge(&mut b, &webdav::records(&a).unwrap()).unwrap();
    let folders = list_folders(&b).unwrap();
    let parent = folders.iter().find(|f| f.name == "Renamed").unwrap().id;
    assert_eq!(
        folders
            .iter()
            .find(|f| f.name == "A child")
            .unwrap()
            .parent_id,
        Some(parent)
    );
    delete_folder_with_feeds(&a, root).unwrap();
    webdav::merge(&mut b, &webdav::records(&a).unwrap()).unwrap();
    assert!(list_folders(&b).unwrap().is_empty());
    assert!(list_feeds(&b).unwrap().is_empty());
    assert_eq!(count_unread(&b).unwrap(), 0);
}
#[test]
fn nested_opml_round_trips_without_losing_parent_names() {
    let conn = database();
    let root = create_folder(&conn, "Parent / literal slash").unwrap();
    let child = create_folder_in(&conn, "Child", Some(root)).unwrap();
    subscribe(&conn, child, "child");
    let xml = crate::opml::build_with_folders(
        &feeds_for_export(&conn).unwrap(),
        &list_folders(&conn).unwrap(),
    )
    .unwrap();
    let feeds = crate::opml::parse(&xml).unwrap();
    assert_eq!(
        feeds[0].folder_path,
        vec!["Parent / literal slash", "Child"]
    );
    let imported = database();
    let id = folder_id_by_path(&imported, &feeds[0].folder_path)
        .unwrap()
        .unwrap();
    assert!(list_folders(&imported)
        .unwrap()
        .iter()
        .find(|f| f.id == id)
        .unwrap()
        .parent_id
        .is_some());
}
