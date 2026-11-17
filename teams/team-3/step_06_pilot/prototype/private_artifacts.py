"""Local development helpers; these do not supply locking or cloud durability."""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: save each raw feed response as a compressed file with a SHA-256
# fingerprint, and read it back only if the fingerprint still matches. This
# makes it possible to prove later that saved inputs were not altered.

## Packages
import gzip,hashlib,json,os,re  # Compression, fingerprints, JSON, disk flush, and ID pattern check
from pathlib import Path  # Build file paths safely


## Calculate the SHA-256 fingerprint of a file
def digest_file(path):
    """Return the SHA-256 hex digest of a file, read in 1 MB blocks."""
    h=hashlib.sha256()  # Create an empty fingerprint
    with open(path,'rb') as source:
        # Read 1 MB at a time so large files do not need to fit in memory.
        for block in iter(lambda:source.read(1024*1024),b''): h.update(block)
    return h.hexdigest()  # Return the fingerprint as 64 hex characters


## Save one feed response pair (station information and station status)
def write_feed_archive(directory, run_id, info, status):
    """Create a new compressed response pair. Caller owns its directory and lifecycle.

    A crash can leave an incomplete file; this is not an atomic publish protocol.
    The raw inputs are decoded JSON values, not original network bytes.
    """
    # Confirm the run ID is a lowercase UUID, so it cannot carry a path such as "../".
    if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',run_id):
        raise ValueError('run_id must be a lowercase UUID')
    # Serialize compactly and deterministically so the same inputs give the same bytes.
    payload=json.dumps({'info':info,'status':status},separators=(',',':'),ensure_ascii=True).encode('utf-8')
    compressed=gzip.compress(payload,mtime=0)  # mtime=0 keeps the gzip header free of the current time
    path=Path(directory)/(run_id+'.json.gz')  # One file per run
    # Mode 'xb' creates the file only if it does not exist, so nothing is overwritten.
    with open(path,'xb') as target:
        target.write(compressed);target.flush();os.fsync(target.fileno())  # Write, then force it to disk
    expected=hashlib.sha256(compressed).hexdigest()  # Fingerprint of what we meant to write
    # Confirm the file on disk matches what was written.
    if digest_file(path)!=expected:raise IOError('archive hash mismatch')
    # Return a small manifest the caller stores alongside the run.
    return {'file_name':path.name,'sha256':expected,'compressed_bytes':len(compressed),'json_bytes':len(payload)}


## Read a saved response pair back, only if it is unchanged
def read_feed_archive(directory, manifest):
    """Load a saved response pair after confirming its fingerprint matches the manifest."""
    name=manifest['file_name']
    # Confirm the manifest names a plain file, not a path into another folder.
    if Path(name).name!=name:raise ValueError('invalid file name')
    path=Path(directory)/name
    # Confirm the file still has the fingerprint recorded when it was written.
    if digest_file(path)!=manifest['sha256']:raise IOError('archive hash mismatch')
    with gzip.open(path,'rt',encoding='utf-8') as source:return json.load(source)  # Return the decoded JSON


## Check whether a checkpoint file is unchanged
def checkpoint_unchanged(path, expected_sha256):
    """Detect changes; NOT an atomic compare-and-swap and NOT a writer lock."""
    return digest_file(path)==expected_sha256  # True when the fingerprint still matches
