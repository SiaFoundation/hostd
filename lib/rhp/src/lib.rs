const SECTOR_SIZE: usize = 1<<22;

#[no_mangle]
pub extern "C" fn sector_root(data: *const u8, output: *mut u8) {
	let sector = unsafe { std::slice::from_raw_parts(data, SECTOR_SIZE) };
	let hash = sia_core::rhp::sector_root(sector);
	let hash_bytes: &[u8; 32] = hash.as_ref();

    unsafe {
        std::ptr::copy_nonoverlapping(hash_bytes.as_ptr(), output, 32);
    }
}