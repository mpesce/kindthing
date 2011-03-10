#!/usr/bin/python
#
# Copyright (c) 2011 Mark D. Pesce
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from google.appengine.ext import db
import things
import time
import logging, urllib
import simplejson as json

class MainHandler(webapp.RequestHandler):
    # GET is used to make queries to the database
    def get(self):
    	logging.info('MainHandler.GET')
    	self.response.headers["Content-Type"] = "text/html"
        key1 = urllib.unquote(self.request.get('k1'))
        key2 = urllib.unquote(self.request.get('k2'))
        
        # OK, construct a query of a possible range of queries
        if ((len(key1) > 0) & (len(key2) > 0)):		# We have both keys
          qstr = 'SELECT * FROM Thing WHERE k1=\'%s\' AND k2=\'%s\'' % (key1, key2)
          q = db.GqlQuery(qstr)
        elif ((len(key1) > 0) & (len(key2) == 0)):		# We have key 1
          qstr = 'SELECT * FROM Thing WHERE k1=\'%s\'' % key1
          #qstr = 'SELECT * from Thing WHERE k1=\'Mark Pesce\''
          logging.debug('qstr: ' + qstr)
          q = db.GqlQuery(qstr)
        elif ((len(key1) == 0) & (len(key2) > 0)):  # We have key 2
          qstr = 'SELECT * FROM Thing WHERE k2=\'%s\'' % key2
          q = db.GqlQuery(qstr)
        else:  # No keys, this is an illegal query
          #self.response.out.write('ILLEGAL QUERY')
          rsp = { "success": "-201", "detail": "ILLEGAL QUERY" }
          self.response.out.write(json.dumps(rsp))
          return
        
        # Build the response as a series of JSON objects, one per query result
        results = q.fetch(limit=10240)  # This may not be enough in certain circumstances
        if len(results) > 0:
          json_entry = []
    	  for p in results:
    	    json_entry.append({"k1": p.k1, "k2": p.k2, "blob": p.blob})
    	  self.response.out.write(json.dumps(json_entry))	  
    	else:
    	  self.response.out.write(json.dumps([]))   # Empty array if there are no matches

    # POST is used to add / update / delete entries in the database
    # the POST body must have command=***JSON object***
    # For example, one might be:
    # command={"k1":"Mark Pesce","k2":"SSN","hash":"408b6a89c4af618598ba3cc53197f3150af13d50eb7038ca806fdcd0e8e2cf4e","blob":"XXX-XX-XXXX"}
	#
    def post(self):
    	logging.info('MainHandler.POST')
    	self.response.headers["Content-Type"] = "text/html"
    	
    	# If it is a POST, everything should be in the body.  We hope.
    	msg = self.request.get('command')
    	logging.info(msg)
    	msg = urllib.unquote(msg)
    	try:
    	  msg_dict = json.loads(msg)
    	except:
    	  logging.error('Could not convert JSON in POST, aborting')
    	  logging.error('SELF: ' + yaml.dump(self))
    	  rsp = { "success": "-101", "detail": "MALFORMED COMMAND -- DATA NOT PRESENT" }
    	  self.response.out.write(json.dumps(rsp))
    	  return
    	#msg_dict = cvt_jsobj_to_dict(msg_obj)		# We should now have a Pythonesque dictionary from the hashes

        # Now extract all the values, which also tells us that we got a fully-formed command.
        try:
          k1 = msg_dict['k1']
          k2 = msg_dict['k2']
          hash = msg_dict['hash']
          blob = msg_dict['blob']
        except:  # Probably a key error
          logging.error('Could not extract values from command, aborting')
          #self.response.out.write('MALFORMED COMMAND -- MISSING DATA')
          rsp = { "success": "-102", "detail": "MALFORMED COMMAND -- DATA NOT PRESENT" }
          self.response.out.write(json.dumps(rsp))
          return
 
        if (len(k1) == 0) | (len(k2) == 0):		# Must have both keys present
          rsp = { "success": "-103", "detail": "MALFORMED COMMAND -- KEY MISSING" }
          self.response.out.write(json.dumps(rsp))
          return        

        if len(hash) == 0:		# Must have a hash, always
          rsp = { "success": "-104", "detail": "MALFORMED COMMAND -- HASH MISSING" }
          self.response.out.write(json.dumps(rsp))
          return        

 
        if len(blob) > 5120:			# Blob too big?
          logging.warning('Blob too big, entry rejected')
          rsp = { "success": "-110", "detail": "COMMAND REJECTED -- BLOB EXCEEDS 5120 BYTES" }
          self.response.out.write(json.dumps(rsp))
          #self.response.out.write('COMMAND REJECTED -- BLOB EXCEEDS 5120 BYTES')
          return

        # Here's how it goes down:
        # Does the entry already exist in the database?
        # If so, do the hashes match?
        # If not, reject any changes
        # If so, is there a blob?
        # If so, update the blob
        # If not, delete the entry
        # If no match, just go ahead and insert the entry
        qstr = 'SELECT * FROM Thing WHERE k1=\'%s\' AND k2=\'%s\'' % (k1, k2)
        q = db.GqlQuery(qstr)
        results = q.fetch(limit=1)
        if (len(results) > 0):		# We do have a match here, actually
          if results[0].hash != hash:   # Do the hashes match?
            logging.warning('Failed to match hash, operation illegal')
            #self.response.out.write('HASHFAIL: OPERATION NOT PERMITTED')
            rsp = { "success": "-120", "detail": "HASHFAIL: OPERATION NOT AUTHORIZED" }
            self.response.out.write(json.dumps(rsp))
            return
          else:
            if len(blob) == 0:	# Are we going to delete the entry?
              results[0].delete()
              logging.warning('Deleted entry')
              rsp = { "success": "1", "detail": "DELETED ENTRY" }
              self.response.out.write(json.dumps(rsp))
              #self.response.out.write('DELETED ENTRY')
              return
            else:
              results[0].blob = blob
              results[0].put()
              logging.warning('Entry updated')
              rsp = { "success": "2", "detail": "UPDATED ENTRY" }
              self.response.out.write(json.dumps(rsp))
              #self.response.out.write('UPDATED ENTRY')
              return
        
        # At long last, let's do an insert.  Please.  Now.  Here.
        try:
          if len(blob) > 0:
            insertable = things.Thing(k1=k1,k2=k2,hash=hash,blob=blob)
            insertable.put()
            logging.warning('Inserted new entry')
            #self.response.out.write('INSERTED ENTRY')
            rsp = { "success": "3", "detail": "INSERTED ENTRY" }
            self.response.out.write(json.dumps(rsp))
          else:
            logging.error('No blob, aborting')
            #self.response.out.write('NO BLOB ERROR')
            rsp = { "success": "-111", "detail": "NO BLOB ERROR" }
            self.response.out.write(json.dumps(rsp))
        except:  # Probably a key error
          logging.error('Could not INSERT into the database, aborting')
          #self.response.out.write('FAILED TO INSERT ENTRY')
          rsp = { "success": "-1", "detail": "UNKNOWN ERROR -- FAILED TO INSERT ENTRY"}
          self.response.out.write(json.dumps(rsp))


# Convert a series of Javascript Hash objects to a dictionary
# A bit of work, but we like Python dictionaries.  A lot.
def cvt_jsobj_to_dict(array):
  rd = {}
  for entry in array:
    l = { entry['name']: entry['value'] }
    rd.update(l) 
  return rd

def main():
    application = webapp.WSGIApplication([('/', MainHandler)], debug=True)
    util.run_wsgi_app(application)

if __name__ == '__main__':
    logging.debug('We are about to invoke main.main')
    #print 'How will this work?'
    main()
