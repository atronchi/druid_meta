import httplib
import json
import logging as log
import requests

'''
e.g. to purge unused old intervals:

d = Druid()
for i in d.get_intervals('algodash_pvr_rank_sketch_test')[:0]:
    d.delete_interval('algodash_pvr_rank_sketch_test', i)
'''

class Druid(object):
    def __init__(self,
            # From go/druid-prod
            coordinator_url='http://bdp_druid-coordinator-prod.dynprod.netflix.net:7103/',
            endpoint='druid/coordinator/v1/',
            verbose=False,
            ):
        self.coordinator_url = coordinator_url
        self.endpoint = endpoint

        if verbose:
            # Debug logging
            httplib.HTTPConnection.debuglevel = 1
            log.basicConfig()
            log.getLogger().setLevel(log.DEBUG)
            req_log = log.getLogger('requests.packages.urllib3')
            req_log.setLevel(log.DEBUG)
            req_log.propagate = True

        self.status = requests.get(coordinator_url + 'status').json()

        #self.datasources = {d: self.get_datasource(d) for d in self.get('datasources')}
        self.datasources = self.get_datasources('full')

    def get(self, path):
        "For valid paths, see http://druid.io/docs/latest/design/coordinator.html"
        r = requests.get(self.coordinator_url + self.endpoint + path)
        return r.json()

    def post(self, path, json):
        r = requests.post(self.coordinator_url + self.endpoint + path, json=json)
        return r.json()

    def delete(self, path):
        r = requests.delete(self.coordinator_url + self.endpoint + path)
        return r.status_code


    def get_loadstatus(self, opt=None):
        "e.g. opt='full' or opt='simple'"
        return self.get('loadstatus{}'.format(('?' + opt) if opt else ''))

    def get_loadqueue(self, opt=None):
        "e.g. opt='full' or opt='simple'"
        return self.get('loadqueue{}'.format(('?' + opt) if opt else ''))


    def get_datasources(self, opt=None):
        "e.g. opt='full' or opt='includeDisabled'"
        return self.get('datasources{}'.format(('?' + opt) if opt else ''))

    def get_datasource(self, datasource):
        return self.get('datasources/' + datasource)


    def get_segments(self, datasource, opt=None, intervals=None):
        "e.g. opt='full'"
        if intervals:
            return self.post('metadata/datasources/{}/segments{}'.format(datasource, ('?' + opt) if opt else ''), json=intervals)
        else:
            return self.get('datasources/{}/segments{}'.format(datasource, ('?' + opt) if opt else ''))

    def get_segment(self, datasource, segment):
        return self.get('datasources/{}/segments/{}'.format(datasource, segment))

    def delete_segment(self, datasource, segment):
        return self.delete('datasources/{}/segments/{}'.format(datasource, segment.replace('/', '_')))


    def get_intervals(self, datasource, opt=None):
        "e.g. opt='simple' or opt='full'"
        return self.get('datasources/{}/intervals{}'.format(datasource, ('?' + opt) if opt else ''))

    def get_interval(self, datasource, interval, opt=None):
        "e.g. opt='simple' or opt='full'"
        return self.get('datasources/{}/intervals/{}{}'.format(datasource, interval.replace('/', '_'), ('?' + opt) if opt else ''))

    def delete_interval(self, datasource, interval):
        for s in [s for s in self.get_segments('algodash_pvr_rank_sketch_test') if interval.replace('/', '_') in s]:
            log.info('{} {}'.format(self.delete_segment(datasource, s), s))
        log.info('{} {}'.format(self.delete('datasources/{}/intervals/{}'.format(datasource, interval.replace('/', '_'))), interval))
