#!/usr/bin/env python

from cfront.utils import genome_db, webserver_db, genome_io, byte_scanner
from cfront.models import Session, SpacerERR, JobERR, Job, Spacer, Hit
import sys, shutil
import time
import transaction
from pyramid.paster import bootstrap
from cfront import cfront_settings, genomes_settings as genomes_settings
import argparse, os
from Queue import Empty
import multiprocessing as mp
from multiprocessing import Process, Manager, Value, Array, JoinableQueue, Queue
import json
import redis
from datetime import datetime
from datetime import timedelta  #added on 3/14/2016
import time
from scipy import misc, power, ceil

redis_key = None
def init_env(p):
    global redis_key
    env = bootstrap(p)
    redis_key = "cfront-{0}:job:hits".format("dev" if cfront_settings.get("debug_mode",False) else "prod")


def queue_loop(ofs,stride):
    while True:
        process_queue(ofs,stride)
        time.sleep(1)
            

def worker(jobs_q,**genomes):
    r = redis.Redis()
    done = False
    while not done:
        job = jobs_q.get()
        if job == 'done':
            done = True
        else:
            print job
            spacerid = job['spacerid']            
            guide = job["guide"]
            genome_name = job["genome_name"]  
            results = genome_db.fetch_hits_in_thread_shm(guide, genome_name, genomes[genome_name])   
            print "SPACER ID IN WORKER is {0}".format(spacerid)
            r.rpush(redis_key,
                    json.dumps({"spacerid": spacerid,
                                "results":results}))
                    
        jobs_q.task_done()
    return

def process_queue(ofs, stride):
    possible_spacer_jobs = Session.query(Job).filter(Job.computed_spacers == False).all()
    selected_jobs = possible_spacer_jobs

    for j in selected_jobs[:100]:
        with transaction.manager:
            try:
                print 'computing spacers {0}'.format(j.id)
                spacer_infos = webserver_db.compute_spacers(j.sequence)  
                if(len(spacer_infos)) == 0:
                    raise JobERR(Job.NOSPACERS, j)
                for spacer_info in spacer_infos:
                    Session.add(Spacer(job = j,**spacer_info))
                j.computed_spacers = True
                Session.add(j)
                print j.id
            except JobERR, e:
                if e.message == Job.NOSPACERS:
                    j.failed = True
                    j.computed_spacers = True
                    Session.add(j)
                    print "No spacers in JOB ID: {0}".format(j.id)
                elif e.message == Job.ERR_INVALID_CHARACTERS:
                    j.failed = True
                    j.computed_spacers = True
                    Session.add(j)
                    print e.message
                else:
                    print "Excepted a job error during Spacer finding for Job: {0}".format(j.id)
                    j.failed = True
                    Session.add(j)
                    print j.id
                    print j.sequence
                    raise e


    possible_hit_jobs =  Session.query(Job)\
                    .join(Spacer)\
                    .filter(Spacer.score == None)\
                    .filter(Job.failed == False)\
                     .all()
#    possible_hit_jobs =  Session.query(Job)\
#                    .filter(Job.failed == False)\
#                    .all()                    .filter(Job.computed_hits == False)\
#                     .all()                     
    selected_hit_jobs = possible_hit_jobs


    procs = []
    max_procs =  4
    manager = Manager()
    jobs_q = JoinableQueue()


    r = redis.Redis()

    dcount = 0 
    while(1):
        item_json = r.lpop(redis_key)
        dcount += 1
        if item_json == None:
            break
    print "popped {0} keys from previous submission".format(dcount)



    for i in range(max_procs):
        print("loading {0}".format(i))
        genomes_dict =dict([n,byte_scanner.get_library_bytes_shm(n) ]
                           for n in genomes_settings["genome_names"])
        proc = mp.Process(target=worker, args=[jobs_q],
                          kwargs = genomes_dict
                            )
        proc.daemon=True
        proc.start()
        procs.append(proc)


    if len(selected_hit_jobs) > 0:
        with transaction.manager:
            def priority(j):
                from random import random
                f = 1 if random() > .5 else -1
#                j_i = datetime.now() - datetime.fromtimestamp(j.submitted_ms/1000)
#                j_i = abs(j_i.total_seconds() - 3600*12)
#                if j.batch is not None: return 100000000
#                elif j_i > (3600*24*2 - 3600*12): return 100000000 - 1
#                elif f > 0: return -j_i
#                else: return j_i
                if j.batch is not None: return 100000000
                else: return f* j.id 

            for i,j in enumerate(selected_hit_jobs):
                if not j in Session: 
                    selected_hit_jobs[i] = Session.merge(j)

            batched_jobs = [j for j in selected_hit_jobs if j.batch is not None]
            #sorts jobs to process recent submissions and non-batch jobs first        
            top_jobs = sorted(selected_hit_jobs, key = priority)[:12]
            for top_job in top_jobs:
                if not top_job in Session: top_job = Session.merge(top_job)

                #spacers may be deleted from the session in the interior of this loop

                print "GENOME NAME: {0}".format(top_job.genome_name)
                print "SAVING ID INTO QUEUE", top_job.spacers[0] 
            
                for i,s in enumerate([s for s in top_job.spacers if s.score is None][:6]):
                    jobs_q.put({"genome_name":s.job.genome_name,
                                "guide":s.guide,
                                "spacerid":s.id})
                
                
    for i in range(max_procs):
        jobs_q.put("done")
    for i,p in enumerate(procs):
        p.join()


    

    print "REDIS KEY IS: {0}".format(redis_key)
    while(1):
       item_json = r.lpop(redis_key)
       
       if item_json == None:
           break
       else:
           item = json.loads(item_json)
           
       print "PROCESSING ITEM"
       with transaction.manager:
          #HANDLE POSSIBLE ERRORS!
          result = item["results"]
          success = result[0]
          hits = result[1]
          sid = item["spacerid"]
          print "SID", sid
          print("completing {0}".format(sid))
          try:
              if not success:
                  #exception handling
                  if hits=="too_many_hits":
                      raise SpacerERR(Spacer.ERR_TOOMANYHITS, Session.query(Spacer).get(sid))
                  elif hits =="no_hits":
                      raise SpacerERR(Spacer.ERR_NO_HITS,Session.query(Spacer).get(sid))
                  elif hits == "failed":
                      raise SpacerERR(Spacer.ERR_FAILED_TO_RETRIEVE_HITS,Session.query(Spacer).get(sid))
              else:
#                  if (sid == 9219511):
#                    print 'PAY ATTENTION HERE\n'
#                    print hits
#                    print len(hits)
#                    print result
#                    print 'PAY ATTENTION HERE\n'
#                  if (sid == 8674715):
#                    print 'PAY ATTENTION HERE\n'
#                    print hits
#                    print len(hits)
#                    print result
#                    print 'PAY ATTENTION HERE\n'
                  genome_db.process_hits_for_spacer(sid, hits)
                  print "DONE PROCESSING JOB"
          except JobERR, e:
              print "excepted a job/spacer error on COMPUTE for job {0}".format(s.job.id, top_job)
          except SpacerERR, e:
              print "excepted a spacer error for spacer id {0}".format(Session.query(Spacer).get(sid))
              print e.message
          except Exception, e:
              
              print "EXCEPTED AN UNKNOWN ERROR {0}".format(sid)
              print "EXCEPTED AN UNKNOWN ERROR"
              print "EXCEPTED AN UNKNOWN ERROR"
              print e.message
              #spc = Session.query(Spacer).get(sid)
    
          #    serr = SpacerERR(Spacer.ERR_FAILED_TO_PROCESS_HITS,Session.query(Spacer).get(sid))  
          #    print serr.message
              
           
       spacer = Session.query(Spacer).get(sid)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reset','-r',dest="reset",
                        default=False, const = True , action="store_const",
                        help = "Reset all jobs (deletes /jobs/*)")
    parser.add_argument('--jobstride',dest="jobstride",
                        default=1,type=int,
                        help="stride of the multithreaded job handler")
    parser.add_argument('--jobofs',dest="joboffset",
                        default=0,type=int,
                        help="offset of this process in the case where stride is >1")

    parser.add_argument('inifile')
    args = parser.parse_args()

    init_env(args.inifile)
    
    print cfront_settings
    queue_loop(args.joboffset, args.jobstride)

init_env('/home/ben/crispr/cfront/production.ini')
possible_hit_jobs =  Session.query(Job)\
                .join(Spacer)\
                .filter(Spacer.score == None)\
                .filter(Job.failed == False)\
                 .all()
selected_hit_jobs = possible_hit_jobs
for i,j in enumerate(selected_hit_jobs):
  if not j in Session: 
      selected_hit_jobs[i] = Session.merge(j)
      
for i,j in enumerate(selected_hit_jobs):
  if j.key == '5341681114019188':
    print i,j.key
    break
top_job = selected_hit_jobs[i]

for i,j in enumerate(selected_hit_jobs):
  print i, j.computed_hits
  if j.computed_hits == False:
    print i,j.key
    break
top_job = selected_hit_jobs[i]

                print "GENOME NAME: {0}".format(top_job.genome_name)
                print "SAVING ID INTO QUEUE", top_job.spacers[0] 

                for i,s in enumerate([s for s in top_job.spacers]):
                  print s.sequence, s.score
            
                for i,s in enumerate([s for s in top_job.spacers if s.score is None]):
                  print s.sequence
                  jobs_q.put({"genome_name":s.job.genome_name,
                              "guide":s.guide,
                              "spacerid":s.id})                   

    for i in range(max_procs):
        jobs_q.put("done")
    for i,p in enumerate(procs):
        p.join()

for n in genomes_settings["genome_names"]:
  print n
                                
    for i in range(max_procs):
        print("loading {0}".format(i))
        genomes_dict =dict([n,byte_scanner.get_library_bytes_shm(n) ]
                           for n in [top_job.genome_name])
        proc = mp.Process(target=worker, args=[jobs_q],
                          kwargs = genomes_dict
                            )
        proc.daemon=True
        proc.start()
        procs.append(proc)                          

import time

start = time.time()
possible_spacer_jobs = Session.query(Job).filter(Job.computed_spacers == False).all()
end = time.time()
print end - start

start = time.time()
possible_hit_jobs =  Session.query(Job)\
                .order_by('-job.id')\
                .join(Spacer)\
                .filter(Spacer.score == None)\
                .filter(Job.failed == False)\
                .limit(100)\
                .all()
end = time.time()
print end - start

start = time.time()
possible_hit_jobs =  Session.query(Job)\
                .filter(Job.failed == False)\
                .order_by('-id')\
                .limit(100)\
                 .all()
end = time.time()
print end - start
print len(possible_hit_jobs)

top_job = possible_hit_jobs[0]
for s in top_job.spacers:
  print s.sequence, s.score
 
Session.flush()
Session.rollback()

for j in possible_hit_jobs:
  print j.date_submitted, j.date_completed, j.computed_hits, j.key, len(j.spacers)
  print d = datetime.now() - j.date_submitted

#check job submission after 1,1
t1 = datetime(2016, 1, 1, 0, 0)

start = time.time()
possible_hit_jobs =  Session.query(Job)\
                .filter(Job.date_submitted > t1)\
                .order_by('-id')\
                 .all()
end = time.time()

dict_jobs = {}
for j in possible_hit_jobs:
  dict_jobs[j.id] = (j.key, j.date_submitted, j.email, j.n_spacers, j.n_completed_spacers, j.genome, j.sequence, j.batchid, j.batch is not None)

list_date_submitted = []
for j in possible_hit_jobs:
  if j.date_submitted not in list_date_submitted:
    list_date_submitted.append(j.date_submitted)

#embedding
(j.date_submitted - t1).days

list_email = [];
for i,j in enumerate(dict_jobs.keys()):
  j_email = dict_jobs[j][2]
  if j_email not in list_email:
    list_email.append(j_email)

list_sequence = [];
for i,j in enumerate(dict_jobs.keys()):
  j_sequence = dict_jobs[j][6]
  if j_sequence not in list_sequence:
    list_sequence.append(j_sequence)

dict_jobs_coded = [];
for j in sorted(dict_jobs.keys()):
  entry = list(dict_jobs[j])
  entry[1] = (entry[1] - t1).days
  entry[2] = list_email.index(entry[2])
  entry[6] = list_sequence.index(entry[6])
  if not entry[7]:
    entry[7] = 0
  entry[8] = int(entry[8])
  dict_jobs_coded.append(entry)

import csv
with open('dict_jobs_coded.csv', 'wb') as f:
    writer = csv.writer(f)
    for j in dict_jobs_coded:
      writer.writerow(j)

with open('list_email.csv', 'wb') as f:
    writer = csv.writer(f)
    for i,j in enumerate(list_email):
      writer.writerow([i,j])

with open('list_sequence.csv', 'wb') as f:
    writer = csv.writer(f)
    for i,j in enumerate(list_sequence):
      writer.writerow([i,j])

from datetime import timedelta
deltatime = timedelta(days = 47)
t1 + deltatime

#set date_completed
for d in range(1,31):
  print d
  is_selected_hit_jobs = True
  while(is_selected_hit_jobs):
    with transaction.manager:
      deltatime = timedelta(days = d)
      t1 = datetime.now() - deltatime
      possible_hit_jobs =  Session.query(Job)\
                      .filter(Job.date_submitted > t1)\
                      .filter(Job.failed == False)\
                      .filter(Job.date_completed == None)\
                      .order_by('-id')\
                      .all()
      if len(possible_hit_jobs) == 0:
        break
      selected_hit_jobs = possible_hit_jobs
      for i,j in enumerate(selected_hit_jobs):
        if not j in Session: 
            selected_hit_jobs[i] = Session.merge(j)
      cj = 0;
      for i,j in enumerate(selected_hit_jobs):
        if j.computed_hits:
          cj = cj + 1
          print cj, i, j
          Session.query(Job)\
          .filter(Job.id == j.id)\
          .update({'date_completed': j.date_submitted})
          
          if cj == 1000:
            Session.flush()
            break
      Session.flush()
      is_selected_hit_jobs = False

#set job failed for a user with too many jobs

th_days = 1
th_jobs = 100
deltatime = timedelta(days = 1)
t1 = datetime.now() - deltatime
possible_hit_jobs =  Session.query(Job)\
                .filter(Job.date_submitted > t1)\
                .filter(Job.failed == False)\
                .filter(Job.date_completed == None)\
                .order_by('-id')\
                .all()                   
selected_hit_jobs = possible_hit_jobs
for i,j in enumerate(selected_hit_jobs):
  if not j in Session: 
    selected_hit_jobs[i] = Session.merge(j)
#count number of jobs per user per day and mark jobs
dict_job_t_u = {}
for i,j in enumerate(selected_hit_jobs):
  t = (j.date_submitted - t1).days
  u = j.email
  jid = j.id
  if (t,u) not in dict_job_t_u:
    dict_job_t_u[(t,u)] = []
  dict_job_t_u[(t,u)].append(jid)
list_job_mark = []
for tu,ljid in dict_job_t_u.items():
  if len(ljid) > th_jobs:
    list_job_mark = list_job_mark + ljid

if len(list_job_mark):
  is_selected_hit_jobs = True
  while(is_selected_hit_jobs):
    with transaction.manager:
      possible_hit_jobs =  Session.query(Job)\
                      .filter(Job.date_submitted > t1)\
                      .filter(Job.failed == False)\
                      .filter(Job.date_completed == None)\
                      .order_by('-id')\
                      .all()                   
      selected_hit_jobs = possible_hit_jobs
      for i,j in enumerate(selected_hit_jobs):
        if not j in Session: 
          selected_hit_jobs[i] = Session.merge(j)
      for cj, jid in enumerate(list_job_mark):
        print cj, jid
        Session.query(Job)\
              .filter(Job.id == jid)\
              .update({'failed': True})
        if cj>= 1000:
          Session.flush()
          break
      Session.flush()
      is_selected_hit_jobs = False
      
from sqlalchemy.dialects import postgresql
q = Session.query(Job)\
          .filter(Job.date_submitted > t1)\
          .filter(Job.failed == False)\
          .filter(Job.date_completed == None)\
          .order_by('-id')
print str(q.statement.compile(dialect=postgresql.dialect()))

#test priority for batch job
def priority(j):
  from random import random
  f = 1 if random() > .5 else -1
  #                j_i = datetime.now() - datetime.fromtimestamp(j.submitted_ms/1000)
  #                j_i = abs(j_i.total_seconds() - 3600*12)
  #                if j.batch is not None: return 100000000
  #                elif j_i > (3600*24*2 - 3600*12): return 100000000 - 1
  #                elif f > 0: return -j_i
  #                else: return j_i
  if j.batch is not None: return 100000000
  else: return f* j.id

x = [(priority(j),j.batchid is not None,j.batch is not None) for j in possible_hit_jobs]
sorted(x, key=lambda y:y[0])[:10]
  
#separately query single and batch jobs
possible_hit_jobs = Session.query(Job)\
                .filter(Job.date_submitted > t1)\
                .filter(Job.failed == False)\
                .filter(Job.date_completed == None)\
                .filter(Job.batchid == None)\
                .order_by('-id')\
                .limit(10)\
                .all()
possible_hit_batch_jobs = Session.query(Job)\
                .filter(Job.date_submitted > t1)\
                .filter(Job.failed == False)\
                .filter(Job.date_completed == None)\
                .filter(Job.batchid != None)\
                .order_by('-id')\
                .limit(10)\
                .all()
selected_hit_jobs = possible_hit_jobs + possible_hit_batch_jobs

#estimate waiting time
    def estimated_waiting_time(self):        
        deltatime = timedelta(days = 7)
        t1 = datetime.now() - deltatime      
        possible_hit_jobs_total = Session.query(Job)\
                        .filter(Job.date_submitted > t1)\
                        .filter(Job.failed == False)\
                        .filter(Job.date_completed == None)\
                        .filter(Job.batchid == None)\
                        .order_by('-id')\
                        .limit(100)\
                        .all()
        ids = [i.id for i in possible_hit_jobs_total]
        if self.id not in ids:
          return 24*60*60
        else:
          m1 = ids.index(self.id)
          m2 = len(possible_hit_jobs_total)
          N = 12
          if (N - 1) <= m1:
            if m1 >= 50:
              p1 = 0.0
              p2 = 0.0
            else:
              p1 = 1.0/2.0*round(sum([scipy.misc.comb(m1,ii) for ii in range(0,N-1 + 1)]))/scipy.power(2,m1)
              p2 = 0.0
          else:
            if (m2-m1) >= 50:
              p1 = 0.5
              p2 = 0.0
            else:
              p1 = 0.5
              p2 = 1.0/2.0*round(sum([scipy.misc.comb(m2-m1,ii) for ii in range(0,N-1-m1 + 1)]))/scipy.power(2,m2-m1)
          p = p1 + p2
          print m1, m2, p1, p2, p
          return int(ceil((self.n_spacers - self.n_completed_spacers)/2.0) * ceil(1.0/p * 60.0))

for i,j in enumerate(possible_hit_jobs_total):
  if 'yinqing' in j.email:
    print i, j.id, j.date_submitted, j.date_completed, j.key, j.estimated_waiting_time/60
    