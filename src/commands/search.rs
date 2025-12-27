use quaid_core::Store;

pub fn run(query: &str, limit: usize, store: &Store) -> anyhow::Result<()> {
    println!("Searching for: {}\n", query);

    let results = store.search(query, limit)?;

    if results.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    println!("Found {} results:\n", results.len());

    for (conv_id, snippet) in results {
        // Get conversation details
        if let Ok(Some(conv)) = store.get_conversation(&conv_id) {
            println!("📝 {}", conv.title);
            println!("   {}", snippet);
            println!("   ID: {}", conv.id);
            println!();
        }
    }

    Ok(())
}
