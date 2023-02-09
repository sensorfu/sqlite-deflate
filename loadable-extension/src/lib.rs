use miniz_oxide::deflate::compress_to_vec;
use miniz_oxide::inflate::decompress_to_vec;

use sqlite3_ext::function::Context;
use sqlite3_ext::{
    sqlite3_ext_fn, sqlite3_ext_main, Connection, Error, FromValue, Result, ValueRef,
};

#[sqlite3_ext_main(persistent)]
fn init(db: &Connection) -> Result<()> {
    db.create_scalar_function("deflate", &DEFLATE_OPTS, deflate)?;
    db.create_scalar_function("inflate", &INFLATE_OPTS, inflate)?;
    Ok(())
}

#[sqlite3_ext_fn(n_args=1, risk_level=Innocuous, deterministic)]
fn deflate(ctx: &Context, args: &mut [&mut ValueRef]) -> Result<()> {
    let val: &[u8] = args[0].get_blob()?;
    let res: Vec<u8> = compress_to_vec(val, 6);
    ctx.set_result(AsRef::<[u8]>::as_ref(&res))
}

#[sqlite3_ext_fn(n_args=1, risk_level=Innocuous, deterministic)]
fn inflate(ctx: &Context, args: &mut [&mut ValueRef]) -> Result<()> {
    let val: &[u8] = args[0].get_blob()?;
    let res: Vec<u8> = decompress_to_vec(val).map_err(|err| Error::Module(err.to_string()))?;
    ctx.set_result(AsRef::<[u8]>::as_ref(&res))
}
