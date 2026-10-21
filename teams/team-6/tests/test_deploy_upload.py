import importlib.util
from pathlib import Path

import pytest


spec=importlib.util.spec_from_file_location('deploy',Path(__file__).parents[1]/'scripts/deploy_three_hour_pilot.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_resumed_upload_reuses_identical_data_and_rejects_changed_data():
    from io import BytesIO
    from types import SimpleNamespace
    class Files:
        uploads=0
        def download(self,path):
            return SimpleNamespace(contents=BytesIO(b'original'))
        def upload(self,*args,**kwargs):
            self.uploads+=1
    files=Files();w=SimpleNamespace(files=files)
    module.upload_immutable(w,'/Volumes/catalog/schema/volume/file',b'original')
    with pytest.raises(ValueError,match='differs'):
        module.upload_immutable(w,'/Volumes/catalog/schema/volume/file',b'changed')
    assert files.uploads==0
