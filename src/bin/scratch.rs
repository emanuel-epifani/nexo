
fn parse_age(s: &str) -> Result<u8, String> {
    let age = s.parse::<u8>().map_err(|_| "invalid age".to_string())?;
    Ok(age)
}

fn main() {
    match parse_age("42") {
        Ok(a) => println!("Age: {}", a),
        Err(e) => println!("Error: {}", e),
    }
}
