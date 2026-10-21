from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path

spec=importlib.util.spec_from_file_location('operations_audit',
    Path(__file__).resolve().parents[1]/'scripts/audit_three_hour_operations.py')
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)


def test_late_retry_is_late_even_if_original_prediction_was_timely():
    assert audit.attempt_timeliness({
        'origin_utc':'2026-10-16T01:00:00+00:00',
        'publication_completed_at_utc':'2026-10-16T00:56:00+00:00',
        'attempt_publication_completed_at_utc':'2026-10-16T01:01:00+00:00'}) is False
    assert audit.attempt_timeliness({'origin_utc':'2026-10-16T01:00:00+00:00',
        'publication_completed_at_utc':'2026-10-16T00:56:00+00:00'}) is None


def test_missing_schedule_slots_allow_grace_and_do_not_count_before_deployment():
    start=datetime(2026,10,16,1,55,4,tzinfo=timezone.utc)
    starts=[start,start+timedelta(hours=2)]
    assert audit.missing_slots(starts,datetime(2026,10,16,4,59,tzinfo=timezone.utc))==[
        '2026-10-16T02:55:00+00:00']
    assert audit.missing_slots(starts,datetime(2026,10,16,5,6,tzinfo=timezone.utc))==[
        '2026-10-16T02:55:00+00:00','2026-10-16T04:55:00+00:00']
