# extremely minimal guerrillamail api specifically for my use case

import json
import requests
import time

class GMSession:
	def __init__(self, api_url='https://api.guerrillamail.com/ajax.php'):
		self.sid_token = None
		self.email_addr = None
		self.last_linkcheck = None
		self.api_url = api_url

	def _getbase(self, f, params={}):
		params.update({'f': f})
		params.update({'sid_token': self.sid_token})
		r = requests.get(self.api_url, params=params)
		r.raise_for_status()
		return r.json()

	def _get(self, f, params={}):
		self._ensure_linked()
		return self._getbase(f, params)

	# ensure the email is still linked with the session id, since after a period
	# of inactivity (they say ~18 mins, the 5 here is conservative), they can become unlinked
	def _ensure_linked(self):
		# it won't expire after only 5 minutes of inactivity
		if self.last_linkcheck is None or time.time() - self.last_linkcheck > 300:
			self._getbase('set_email_user', {'email_user': self.email_addr.split('@')[0]})
		self.last_linkcheck = time.time()

	def get_email_address(self):
		j = self._getbase('get_email_address')
		self.sid_token = j['sid_token']
		self.email_addr = j['email_addr']
		self.last_linkcheck = time.time()
		# flush the welcome email from the check_email stuff
		j = self.get_email_list()
		self._get('del_email', {'email_ids[]':1})
		return self.email_addr

	def get_email_list(self, offset=0):
		return self._get('get_email_list', {'offset': offset})

	def get_new_emails(self):
		return self._get('check_email', {'seq': 0})

	def get_email(self, id):
		return self._get('fetch_email', {'email_id': id})

	def get_older_list(self, id):
		return self._get('get_older_list', {'seq': id})

	def del_emails(self, ids):
		return self._get('del_email', {'email_ids[]': ids})

	def set_email_user(self, usr):
		return self._get('set_email_user', {'email_user': usr})

	# get all emails at the exact time that you called the function
	def get_all_emails(self):
		j = self.get_email_list()
		l = j['list']
		while int(j['count']) > 20:
			# just in case, don't want to get rate limited
			time.sleep(1)
			j = self.get_older_list(l[-1]['mail_id'])
			l += j['list']
		return l
