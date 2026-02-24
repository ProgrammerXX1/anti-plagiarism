#!/usr/bin/env python3
"""
Offline converter: rewrite v2 index_native.bin → v3 in-place.

v2 Posting9 = (h:u64, did:u32, pos:u32) = 16 bytes
v3 Posting9 = (h:u32, did:u32, pos:u32) = 12 bytes

After truncating h64→h32, the sort order changes, so we re-sort by (h,did,pos).
"""

import struct, os, sys, glob, time
import numpy as np

MAGIC  = b'PLAG'
HDR    = 28        # magic(4)+ver(4)+n_docs(4)+n_post9(8)+n_post13(8)
DMETA  = 20        # per-doc metadata bytes

V2_POST = 16
V3_POST = 12

V2_DTYPE = np.dtype([('h','<u8'),('did','<u4'),('pos','<u4')])   # 16 bytes
V3_DTYPE = np.dtype([('h','<u4'),('did','<u4'),('pos','<u4')])   # 12 bytes


def convert_one(path: str, *, dry_run: bool = False) -> bool:
    """Convert a single file.  Returns True if converted."""
    with open(path, 'rb') as f:
        hdr = f.read(HDR)
    if len(hdr) < HDR:
        print(f"  SKIP (short header): {path}")
        return False
    magic = hdr[:4]
    ver, n_docs = struct.unpack_from('<II', hdr, 4)
    n_post9, n_post13 = struct.unpack_from('<QQ', hdr, 12)

    if magic != MAGIC:
        print(f"  SKIP (bad magic): {path}")
        return False
    if ver == 3:
        print(f"  SKIP (already v3): {path}")
        return False
    if ver != 2:
        print(f"  SKIP (version {ver}): {path}")
        return False

    disk = os.path.getsize(path)
    expected_v2 = HDR + n_docs * DMETA + n_post9 * V2_POST
    if disk != expected_v2:
        print(f"  SKIP (size mismatch {disk} vs expected {expected_v2}): {path}")
        return False

    print(f"  {path}")
    print(f"    n_docs={n_docs:,}  n_post9={n_post9:,}  ({disk/(1<<30):.2f} GiB → ~{(HDR + n_docs*DMETA + n_post9*V3_POST)/(1<<30):.2f} GiB)")

    if dry_run:
        return True

    t0 = time.monotonic()

    # Read v2 postings via mmap for speed
    post_offset = HDR + n_docs * DMETA
    v2 = np.memmap(path, dtype=V2_DTYPE, mode='r', offset=post_offset, shape=(n_post9,))

    # Convert
    v3 = np.empty(n_post9, dtype=V3_DTYPE)
    v3['h']   = v2['h'].astype(np.uint32)
    v3['did'] = v2['did']
    v3['pos'] = v2['pos']
    del v2  # release mmap

    t1 = time.monotonic()
    print(f"    read+convert: {t1 - t0:.1f}s")

    # Sort by (h, did, pos)  — lexsort key order is rightmost = primary
    order = np.lexsort((v3['pos'], v3['did'], v3['h']))
    v3_sorted = v3[order]
    del v3, order

    t2 = time.monotonic()
    print(f"    sort: {t2 - t1:.1f}s")

    # Write to temp file, then atomic rename
    tmp = path + '.v3tmp'
    with open(path, 'rb') as fin, open(tmp, 'wb') as fout:
        # v3 header
        fout.write(struct.pack('<4sII', MAGIC, 3, n_docs))
        fout.write(struct.pack('<QQ', n_post9, n_post13))
        # docmeta (unchanged)
        fin.seek(HDR)
        fout.write(fin.read(n_docs * DMETA))
        # postings
        fout.write(v3_sorted.tobytes())
    del v3_sorted

    os.replace(tmp, path)
    new_size = os.path.getsize(path)
    expected_v3 = HDR + n_docs * DMETA + n_post9 * V3_POST
    ok = new_size == expected_v3

    t3 = time.monotonic()
    status = "OK" if ok else f"SIZE MISMATCH {new_size} vs {expected_v3}"
    print(f"    write+rename: {t3 - t2:.1f}s  total: {t3 - t0:.1f}s  → {new_size/(1<<30):.2f} GiB  [{status}]")
    return ok


def main():
    dry_run = '--dry-run' in sys.argv
    root = '/home/oysyn/Date_Index/anti-plagiarism/Back_Last/DATA_ROOT_DEV'
    files = sorted(glob.glob(root + '/**/index_native.bin', recursive=True))
    print(f"Found {len(files)} index_native.bin files")
    if dry_run:
        print("(DRY RUN — no files will be modified)")

    ok = skip = fail = 0
    t_all = time.monotonic()
    for i, f in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}]")
        r = convert_one(f, dry_run=dry_run)
        if r:
            ok += 1
        else:
            # Could be skip (already v3) or fail
            with open(f, 'rb') as fh:
                ver = struct.unpack_from('<I', fh.read(8), 4)[0]
            if ver == 3:
                skip += 1
            else:
                fail += 1

    elapsed = time.monotonic() - t_all
    print(f"\n=== DONE in {elapsed:.0f}s ===")
    print(f"  converted: {ok}  skipped(v3): {skip}  failed: {fail}")


if __name__ == '__main__':
    main()
