import json,sqlite3,unittest
from pilot_loop import init
from dashboard_view import report
class DashboardTest(unittest.TestCase):
 def test_pending_and_baseline(self):
  with sqlite3.connect(':memory:') as db:
   init(db)
   for sid,c in [('a',1),('b',2)]:
    p={'station_id':sid,'outcome':'bike','fetched_at':'2026-09-28T12:00:00+00:00','current_count':c}
    db.execute('insert into predictions values(?,?,?,?,?)',('r',sid,'bike',json.dumps(p),int(sid=='b')))
   page=report(db)
   self.assertIn('Development monitoring',page);self.assertIn('<td>low count</td><td>2</td>',page);self.assertIn('<td>model</td><td>1</td>',page)
 def test_final_statuses_and_run_health(self):
  with sqlite3.connect(':memory:') as db:
   init(db);db.execute('insert into runs values(?,?,?,?,?,?)',('r','2026-09-28T12:00:00+00:00','2026-09-28T12:00:00+00:00',None,'success','{}'))
   for sid,status,label in [('a','known',1),('b','known',0),('c','unknown',None),('d','pending',None)]:
    p={'station_id':sid,'outcome':'bike','fetched_at':'2026-09-28T12:00:00+00:00','current_count':1}
    db.execute('insert into predictions values(?,?,?,?,?)',('r',sid,'bike',json.dumps(p),1))
    db.execute('insert into outcomes values(?,?,?,?)',('r',sid,'bike',json.dumps({'status':status,'label':label})))
   page=report(db)
   self.assertIn('success: 1',page)
   self.assertIn('<td>bike</td><td>model</td><td>4</td><td>1</td><td>1</td><td>1</td><td>1</td>',page)

class GroupPanelTest(unittest.TestCase):
 def test_group_capture(self):
  with sqlite3.connect(':memory:') as db:
   init(db)
   rows=[('a',1,1,1),('b',2,0,1),('c',3,1,0),('d',9,0,1),('e',8,0,0),('f',7,0,0)]
   for sid,count,alert,label in rows:
    p={'station_id':sid,'outcome':'bike','fetched_at':'2026-09-28T12:00:00+00:00','current_count':count}
    db.execute('insert into predictions values(?,?,?,?,?)',('r',sid,'bike',json.dumps(p),alert))
    db.execute('insert into outcomes values(?,?,?,?)',('r',sid,'bike',json.dumps({'status':'known','label':label})))
   groups={'poverty_share':{'a':'lower','b':'lower','c':'middle','d':'higher','e':'higher'}}
   page=report(db,groups)
   self.assertIn('Capture by neighborhood group',page)
   self.assertIn('<td>bike</td><td>model</td><td>lower*</td><td>2</td><td>1</td><td>50.0%</td>',page)
   self.assertIn('<td>bike</td><td>model</td><td>higher*</td><td>1</td><td>0</td><td>0.0%</td>',page)
   self.assertIn('<td>missing*</td>',page)
 def test_no_groups_no_panel(self):
  with sqlite3.connect(':memory:') as db:
   init(db); self.assertNotIn('Capture by neighborhood group',report(db))
if __name__=='__main__':unittest.main()
