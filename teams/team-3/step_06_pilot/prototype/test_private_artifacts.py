import tempfile,unittest
from pathlib import Path
from private_artifacts import write_feed_archive,read_feed_archive,digest_file,checkpoint_unchanged

class PrivateArtifactsTest(unittest.TestCase):
 def test_compressed_roundtrip_and_tamper_detection(self):
  with tempfile.TemporaryDirectory() as folder:
   rid='01234567-89ab-cdef-0123-456789abcdef'
   original={'info':{'stations':['a']*100},'status':{'count':2}}
   manifest=write_feed_archive(folder,rid,original['info'],original['status'])
   self.assertEqual(read_feed_archive(folder,manifest),original)
   self.assertLess(manifest['compressed_bytes'],manifest['json_bytes'])
   with self.assertRaises(FileExistsError):write_feed_archive(folder,rid,{}, {})
   with open(Path(folder)/manifest['file_name'],'ab') as f:f.write(b'x')
   with self.assertRaises(IOError):read_feed_archive(folder,manifest)
 def test_hash_check_detects_change(self):
  with tempfile.TemporaryDirectory() as folder:
   p=Path(folder)/'db';p.write_bytes(b'old'); expected=digest_file(p)
   self.assertTrue(checkpoint_unchanged(p,expected))
   p.write_bytes(b'new');self.assertFalse(checkpoint_unchanged(p,expected))
if __name__=='__main__': unittest.main()
