"""S3Store adapter against a stub boto3 client that mimics S3 conditional-write errors (no network)."""
import unittest
from botocore.exceptions import ClientError
import s3_publisher as pub


class StubS3:
    def __init__(self): self.obj = {}; self.calls = []
    def head_object(self, Bucket, Key):
        if Key not in self.obj: raise ClientError({'Error': {'Code': '404', 'Message': 'Not Found'}}, 'HeadObject')
        return {}
    def get_object(self, Bucket, Key):
        import io; return {'Body': io.BytesIO(self.obj[Key])}
    def put_object(self, Bucket, Key, Body, IfNoneMatch, ContentType, Metadata):
        self.calls.append((Key, IfNoneMatch, ContentType, Metadata))
        if Key in self.obj: raise ClientError({'Error': {'Code': 'PreconditionFailed', 'Message': 'At least one of the pre-conditions you specified did not hold'}}, 'PutObject')
        self.obj[Key] = Body


class S3StoreTest(unittest.TestCase):
    def test_conditional_put_and_errors(self):
        c = StubS3(); s = pub.S3Store(c)
        k = pub.APPROVED_PREFIX + 'a.csv'
        self.assertFalse(s.exists(k)); s.put_if_absent(k, b'x', 'text/csv'); self.assertTrue(s.exists(k))
        self.assertEqual(c.calls[0][1], '*'); self.assertEqual(c.calls[0][3], {'data-kind': 'PILOT'})
        with self.assertRaises(pub.AlreadyExists): s.put_if_absent(k, b'y', 'text/csv')
        self.assertEqual(c.obj[k], b'x')

    def test_write_files_through_adapter(self):
        c = StubS3(); s = pub.S3Store(c); k = pub.APPROVED_PREFIX + 'outcomes/date=2026-11-12/outcomes_20261112_am.csv'
        r = pub.write_files({k: b'a,b\n1,2\n'}, s); self.assertEqual(r[k]['action'], 'written')
        r = pub.write_files({k: b'a,b\n1,3\n'}, s); self.assertEqual(r[k]['target'], k.replace('.csv', '_v2.csv'))
        self.assertEqual(c.obj[k], b'a,b\n1,2\n')

    def test_other_errors_propagate(self):
        class Denied(StubS3):
            def head_object(self, Bucket, Key): raise ClientError({'Error': {'Code': '403', 'Message': 'Forbidden'}}, 'HeadObject')
        with self.assertRaises(ClientError): pub.S3Store(Denied()).exists(pub.APPROVED_PREFIX + 'x')


if __name__ == '__main__':
    unittest.main()
