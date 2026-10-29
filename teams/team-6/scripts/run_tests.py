"""Run tests with macOS/Miniforge readline disabled; retain safe spawn imports."""
import sys

sys.modules['readline']=None

if __name__=='__main__':
    import pytest
    raise SystemExit(pytest.main(sys.argv[1:] or ['-q']))
