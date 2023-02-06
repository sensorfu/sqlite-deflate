use std::io;

use miniz_oxide::deflate::compress_to_vec;
use miniz_oxide::inflate::decompress_to_vec;
use rusqlite::{functions::FunctionFlags, Connection};

pub fn add_deflate_functions(db: &Connection) -> rusqlite::Result<()> {
    db.create_scalar_function(
        "deflate",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| Ok(compress_to_vec(ctx.get_raw(0).as_bytes()?, 6)),
    )?;

    db.create_scalar_function(
        "inflate",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| decompress_to_vec(ctx.get_raw(0).as_bytes()?).map_err(err_to_rusqlite),
    )?;

    Ok(())
}

fn err_to_rusqlite(err: miniz_oxide::inflate::DecompressError) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(io::Error::new(
        io::ErrorKind::Other,
        format!("DEFLATE decompression failed: {:?}", err.status),
    )))
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::add_deflate_functions;

    #[test]
    fn test_deflate() -> rusqlite::Result<()> {
        let db = Connection::open_in_memory()?;
        add_deflate_functions(&db)?;

        let _b = db.query_row(r##"SELECT deflate("aaaaaaaaa")"##, [], |row| {
            let b: Vec<u8> = row.get(0)?;
            Ok(b)
        })?;

        Ok(())
    }

    #[test]
    fn test_inflate() -> rusqlite::Result<()> {
        let db = Connection::open_in_memory()?;
        add_deflate_functions(&db)?;

        db.execute("CREATE TABLE testi (bloerp);", [])?;
        db.execute("INSERT INTO testi VALUES(deflate('aaaaaaaaa'));", [])?;
        let bytes = db.query_row(r"SELECT inflate(bloerp) FROM testi", [], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(bytes)
        })?;

        let s = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(s, "aaaaaaaaa");

        Ok(())
    }

    quickcheck::quickcheck! {
        #[test]
        fn prop_deflate_decompress(xs: String) -> bool {
            let db = Connection::open_in_memory().unwrap();
            add_deflate_functions(&db).unwrap();

            db.execute("CREATE TABLE testi (bloerp);", []).unwrap();
            db.execute("INSERT INTO testi VALUES(deflate(?));", [&xs]).unwrap();

            let res: String = db.query_row(r"SELECT inflate(bloerp) FROM testi", [], |row| {
                let bytes = row.get(0)?;
                Ok(String::from_utf8(bytes).unwrap())
            }).unwrap();

            xs == res
        }
    }
}
