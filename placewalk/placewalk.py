from __future__ import division
from itertools import repeat, chain
import time
import re
import logging
logging.basicConfig(datefmt="%d/%b/%Y %H:%M:%S", format="%(levelname)s | %(message)s")
console = logging.StreamHandler()
console.setLevel(logging.INFO)

import os


FOURSQUARE_CLIENT_ID     = os.environ.get('FOURSQUARE_CLIENT_ID')
FOURSQUARE_CLIENT_SECRET = os.environ.get('FOURSQUARE_CLIENT_SECRET')
FACTUAL_V3_OAUTH_KEY     = os.environ.get('FACTUAL_V3_OAUTH_KEY')
FACTUAL_V3_OAUTH_SECRET  = os.environ.get('FACTUAL_V3_OAUTH_SECRET')
GOOGLE_API_KEY           = os.environ.get('GOOGLE_API_KEY')

from factual import Factual, APIException
from factual.utils import point
client_factual    = Factual(FACTUAL_V3_OAUTH_KEY,FACTUAL_V3_OAUTH_SECRET)
import foursquare
client_foursquare = foursquare.Foursquare(client_id=FOURSQUARE_CLIENT_ID, client_secret=FOURSQUARE_CLIENT_SECRET,version='20140701')
from googleplaces import GooglePlaces, lang
import googleplaces
client_google     = GooglePlaces(GOOGLE_API_KEY)
import redis
redis_server = redis.Redis(host='localhost', port=6379, db=0)


place_key     = 'crosswalk:place:%s:%s'
places_key    = 'crosswalk:places:%s'
category_key  = 'crosswalk:category:%s' 
categories_key= 'crosswalk:categories'
chain_key_factual = 'crosswalk:chain:factual:%s' # <chain_id> : set of factual ids
chain_key_foursquare = 'crosswalk:chain:foursquare:%s' # <chain_id> : set of foursquare ids
chains_key    = 'crosswalk:chains'
mapped_key = 'crosswalk:mapped:%s' # e.g. feed: {22, 23, 24...}, place:{53}, chain:{22}


data_providers = ['foursquare','factual','google','google_old']
allowed_data   = data_providers + ['name','phone','category_name','category_id','chain_id', 'chain_name', 'latitude','longitude']

def slugify(data):
    data = data.lower()
    return re.sub(r'\W+','-',data)

def get_category_map(data, output = {}):
    """
    Flatten 4sq categories
        {id:slugified-name, ...}
    Usage:
        data            = client.venues.categories()
        category_id_map = get_category_map(data)
    """
    try:
        categories = data.get('categories') or []
        for category in categories:
            name   = slugify(category.get('name'))
            cat_id = category.get('id')
            output.update({cat_id:name})
            get_category_map(category, output=output)
    except:
        pass
    return output

def izip_longest(*args, **kwargs):
    from itertools import repeat, chain
    # izip_longest('ABCD', 'xy', fillvalue='-') --> Ax By C- D-
    fillvalue = kwargs.get('fillvalue')
    counter = [len(args) - 1]
    def sentinel():
        if not counter[0]:
            raise Exception
        counter[0] -= 1
        yield fillvalue
    fillers = repeat(fillvalue)
    iterators = [chain(it, sentinel(), fillers) for it in args]
    try:
        while iterators:
            yield tuple(map(next, iterators))
    except Exception:
        pass


def reset_category_mapping():
    """
    Running ``convert_data`` screws up category mapping on data
    This fixes it
    """
    data            = client_foursquare.venues.categories()
    category_id_map = get_category_map(data)
    for k, v in category_id_map.items():
        redis_server.set(category_key % k, v)
        redis_server.set(category_key % v, k)
    # GO THROUGH EACH PLACE AND FIX THE CATEGORY DATA
    for key in redis_server.keys(place_key % ("*","*")):
        category_id = redis_server.hget(key, 'category_id')
        if category_id:
            current  = redis_server.hget(key, 'category_name')
            expected = redis_server.get(category_key % category_id)
            if current != expected:
                print 'changing %s to %s' % (current, expected)
                redis_server.hset(key, 'category_name', expected)


def convert_data(place_id=None):
    """
    Convert (and delete) the existing redis data analytics

    Get 
    crosswalk:place:<foursquare_id>
    crosswalk:place:<place_id>:<id>
    """
    data_providers = ['foursquare','factual','google','google_old']
    if place_id is None:
        set_of_keys = redis_server.keys('crosswalk:place:*')
    else:
        set_of_keys = redis_server.keys('crosswalk:place:*%s*' % place_id)

    for key in set_of_keys:
        keys_to_delete = []
        print 'on key: %s' % key
        keys_to_delete.append(key)
        # SKIP KEYS THAT HAVE DATA PROVIDER NAMESPACES
        #   e.g. crosswalk:place:foursquare:<id>
        for segment in key.split(':'):
            if segment in data_providers:
                continue
        # GET HASH FROM REDIS
        data          = redis_server.hgetall(key)
        data_provider = data.get('data_provider')
        data_id       = data.get('place_id')
        crosswalk_id  = data.get('crosswalk_id')
        crosswalk_data = {}
        # HANDLE CROSSWALK
        if crosswalk_id:
            for cwkey in redis_server.keys('*%s*' % crosswalk_id):
                if cwkey.startswith('crosswalk:place:'):
                    print '\tcw data: %s' % cwkey
                    keys_to_delete.append(cwkey)
                    crosswalk_data.update(redis_server.hgetall(cwkey))        
        # GET CATEGORY DATA
        category_id   = data.get('cat_id') or crosswalk_data.get('cat_id')
        category_name = data.get('cat')    or crosswalk_data.get('cat')
        if category_id:
            data.update({'category_id':category_id})
        if category_name:
            data.update({'category_name':category_name})
        # FOURSQUARE
        if data_provider == 'foursquare':
            print '\tupdating with 4sq'
            data.update({'foursquare':data_id})
            if crosswalk_data.get('id'):
                data.update({'google_old':crosswalk_data.get('id')})
            if crosswalk_data.get('place_id'):
                data.update({'google':crosswalk_data.get('place_id')})
        # GOOGLE/GOOGLE OLD
        elif data_provider == 'google':
            print '\tupdating with google'
            if data.get('place_id'):
                data.update({'google':data.get('place_id')})
            if data.get('id'):
                data.update({'google_old':data.get('id')})
            if crosswalk_data.get('place_id'):
                data.update({'foursquare':crosswalk_data.get('place_id')})
        
        # DELETE OLD DATA
        for k in keys_to_delete:
            print '\tdeleting: %s' % k
            redis_server.delete(k)
        place = Place(data=data)
        place.save()




class Place(object):
    """
    TO DO:
        change self.data to 
            self.fetched[self.data_provider] and/or self.stored

    place:foursquare:<id> : {'foursquare':<id>, 'google':<id>, 'google_old':<old-id>, factual:<id>, 'name':<name>, 'phone':<phone>, 'category':<category>, 'category_id':<cat_id>, 'chain_id':<factual_chain_id>}
    place:factual:<id>    : {'foursquare':<id>, 'google':<id>, 'google_old':<old-id>, factual:<id>, 'name':<name>, 'phone':<phone>, 'category':<category>, 'category_id':<cat_id>, 'chain_id':<factual_chain_id>}
    place:google:<id>     : {'foursquare':<id>, 'google':<id>, 'google_old':<old-id>, factual:<id>, 'name':<name>, 'phone':<phone>, 'category':<category>, 'category_id':<cat_id>, 'chain_id':<factual_chain_id>}
    place:google_old:<id> : {'foursquare':<id>, 'google':<id>, 'google_old':<old-id>, factual:<id>, 'name':<name>, 'phone':<phone>, 'category':<category>, 'category_id':<cat_id>, 'chain_id':<factual_chain_id>}    

    place = Place(data_provider='foursquare', data_id='4cbcafab035d236aebebe64e',skip_cache=True)
    place = Place(data_provider='foursquare', data_id='4cbcafab035d236aebebe64e')
    place = Place(data_provider='google',     data_id='ChIJPb7KLYdZwokRIgG4nSNM6qg')
    place = Place(data_provider='google_old', data_id='c3cba48cb7aeba0c88258d28f933f0230fff72d6')
    place = Place(data_provider='factual',    data_id='13e1b2f6-e492-4c60-b379-c231aaa83ff6')
    place = Place(data_provider='google',     name='Hotel Chantelle', phone='2122549100')
    place = Place(data_provider='foursquare', name='Hotel Chantelle', phone='2122549100')
    place.save()

    [Place(data_provider=k, data_id=v) for k,v in place.crosswalk_map.items()]

    TMRW:
    Figure out why place name is overwritten 
    place = Place(data_provider='foursquare', data_id='421a7600f964a5209d1f1fe3', skip_cache=True)

    {'category': u'art-museum',
     'category_id': u'4bf58dd8d48988d18f941735',
     'foursquare': u'421a7600f964a5209d1f1fe3',
     'latitude': 40.77362529748567,
     'longitude': -73.9641058682518,
     'name': u'Whitney Museum of American Art',
     'phone': u'2125703600'}
    """
    def __init__(self, **kwargs):
        """
        self.combined()
            combines local and fetched using priority to override 
        if you send in data
            no fetch, yes format 
        if you send in entity, 
            no fetch, yes format
        if you send in data_provider,data_id  || name, phone, data_provider
            fetch
        """
        self.data_provider = kwargs.get('data_provider')
        self.data_id       = kwargs.get('data_id')
        self.fetched       = {} # FETCHED FROM REDIS, DB, CLOUD
        self.local         = {} # PASSED IN AS ENTITY OR DICT
        self.logger        = logging.getLogger('placewalk')
        self.logger.addHandler(console)
        self.logger.setLevel(logging.INFO)

        if not (FOURSQUARE_CLIENT_ID or FOURSQUARE_CLIENT_SECRET
            or FACTUAL_V3_OAUTH_KEY or FACTUAL_V3_OAUTH_SECRET
            or GOOGLE_API_KEY):
            raise Exception("You must set your API KEYS in your local environment.")


        # USED TO INFLATE AND SAVE WITHOUT FETCHING
        if kwargs.get('data'):
            self.local  = self.format(kwargs.get('data'))
        else:            
            self.fetched[self.data_provider] = self.fetch(**kwargs)
    
    def fetch(self, **kwargs):
        """
        If you fetch
            then format and store in object.fetched 
            save 


        Get data from 
            redis      - cached by dp/di 
            mysql      - cached by dp/di 
            
            foursquare - place id search 
            google     - place id search 
            factual    - place id search 

            foursquare - place attrs search 
            google     - place attrs search 
            factual    - place attrs search 
        """
        results = {}
        # GET FROM CACHE: select whichever one is not None
        data_provider = kwargs.get('data_provider')
        data_id       = kwargs.get('data_id')
        if not kwargs.get('skip_cache'):
            cached = self.get_cache(data_provider=data_provider, data_id=data_id)
            if cached:
                self.fetched[data_provider] = cached
                return self.fetched[data_provider]
        self.logger.debug('fetch: from data')
        # FOURSQUARE            
        if data_id and data_provider =='foursquare':
            response = client_foursquare.venues(data_id)
            results  = response.get('venue')
        # GOOGLE
        elif data_id and data_provider=='google':
            results = client_google.get_place(None,place_id=data_id)
        elif data_id and data_provider=='factual':
            results = client_factual.get_row('places', data_id)
        # FOURSQUARE MATCH: lat/lng, name, phone
        elif not data_id and data_provider == 'foursquare':
            response  = client_foursquare.venues.search(params={
                'intent':'match',
                'll':'%s,%s' % (kwargs.get('latitude'),kwargs.get('longitude')),
                'query':kwargs.get('name'),
                'phone':kwargs.get('phone')
                }
            )
            if response.get('venues'):
                results = response.get('venues')[0]
        # GOOGLE PLACES SEARCH 
        elif not data_id and data_provider == 'google':
            keywords = []
            # Get lat/lng
            lat_lng   = {'lat':kwargs.get('latitude'), 'lng':kwargs.get('longitude')}
            # Set of search terms    
            if kwargs.get('phone'):
                keywords.append(kwargs.get('phone'))
            if kwargs.get('name'):
                keywords.append(kwargs.get('name'))
            if kwargs.get('phone') and kwargs.get('name'):
                keywords.insert(0, "{0} {1}".format(kwargs.get('name'),kwargs.get('phone')))
            # Crosswalk to Google: clean up 
            for kw in keywords:
                if kwargs.get('longitude') and kwargs.get('latitude'):
                    query_result = client_google.nearby_search(lat_lng=lat_lng,keyword=kw)
                else:
                    query_result = client_google.text_search(query=kw)
                if len(query_result.places)>=1:
                    # GET FROM GOOGLE AND FORMAT
                    results = query_result.places[0]
                    # STORE RESULT - THIS IS FIRST TIME WE SEE IT
                    break
        elif not data_id and data_provider=='factual':
            try:
                results = client_factual.table('places').search(kwargs.get('name')).geo(point(kwargs.get('latitude'),kwargs.get('longitude'))).data()
            except APIException, e:
                self.logger.exception(e)
                # CHECK IF IT HAS HAPPENED IN THE LAST 60 SECONDS
                if redis_server.get('crosswalk:error:factual:RateLimitExceeded'):
                    self.logger.error("Sleeping 1 hour: RateLimitExceeded\n%s" % time.ctime())
                    time.sleep(60*60+20)
                else:
                    self.logger.error("Sleeping 1 minute: RateLimitExceeded")
                    time.sleep(65)
                    redis_server.set('crosswalk:error:factual:RateLimitExceeded', 1)
                    redis_server.expire('crosswalk:error:factual:RateLimitExceeded', 65)                    
                # SET KEY TO NOTIFY THAT IT HAPPENED
                # SLEEP FOR 1 MINUTE
                results = {}
            if results:
                results = results[0]
            else:
                results = {}
        self.raw = results
        if isinstance(results, list):
            self.logger.error(self.combined())
        self.fetched[data_provider] = self.clean(self.format(results),filters=allowed_data)
        self.save()
        return self.fetched[data_provider]
    
    def save(self, data=None, priority=None):
        """
        use the combined data with priority to save each payload
        """
        if data is None:
            data = self.combined(priority=priority)

        # CHAIN
        if data.get('chain_id') and data.get('chain_id')!='None' and data.get('factual'):
            self.logger.debug('\tsaving factual ids to chain and set of chain ids')
            redis_server.sadd(chain_key_factual % data.get('chain_id'), data.get('factual'))
            redis_server.sadd(chains_key, data.get('chain_id'))
        # CATEGORY 
        if data.get('category_id') and data.get('category_id')!='None' and data.get('category_name') and data.get('category_name')!='None':
            self.logger.debug('\tsaving category id to name map and set')
            redis_server.set(category_key % data.get('category_id'), data.get('category_name'))
            redis_server.set(category_key % data.get('category_name'), data.get('category_id'))
            redis_server.sadd(categories_key, data.get('category_id'))
            redis_server.sadd(categories_key, data.get('category_name'))
        # SAVE FOURSQUARE PLACES BY CATEGORY
        #   crosswalk:places:<category_id>  : set of all places sharing that category id
        if data.get('foursquare') and data.get('category_id') and data.get('category_id')!='None':
            self.logger.debug('\tsaving set of place ids for given category')
            redis_server.sadd(places_key % data.get('category_id'), data.get('foursquare'))

        saved_for = []
        for data_provider in data_providers:
            # PLACE DATA
            if data.get(data_provider) and data.get(data_provider)!='None':
                saved_for.append(data_provider)
                try:
                    data_out = self.combined(priority=data_provider)
                except:
                    data_out = self.combined(priority='foursquare')
                # crosswalk:place:foursquare:<id> : {data}
                redis_server.hmset(place_key % (data_provider, data.get(data_provider)), data_out)
                self.logger.debug('\tsave.places.%s.%s' % (places_key % data_provider, data.get(data_provider)))
                # crosswalk:places:foursquare : set [ <id>, ... ] 
                redis_server.sadd(places_key % data_provider, data.get(data_provider))
        self.logger.debug('saving place data and set of place ids for provider for')
        for x in saved_for:
            self.logger.debug('\t%s' % x)

    def crosswalk(self, data_provider, **kwargs):
        """
        always add data to .fetched

        if looking for chain id:
            try to get from .combined
            do factual search 
            return crosswalk_id
        else:
            look in mysql 
            look in object 
            use factual 
            do fetch search 
            return crosswalk_id

        """
        crossed   = None
        name      = self.combined(priority=self.data_provider).get('name')
        phone     = self.combined(priority=self.data_provider).get('phone')
        latitude  = self.combined(priority=self.data_provider).get('latitude')
        longitude = self.combined(priority=self.data_provider).get('longitude')

        # WE DIDN'T FIND CHAIN ID IN .combined, CHECK FACTUAL
        get_chain = kwargs.get('get_chain')
        if get_chain:
            chain_data = self.fetch(data_provider='factual', name=name, phone=phone, latitude=latitude, longitude=longitude, skip_cache=kwargs.get('skip_cache'))
            self.fetched['factual'].update(chain_data)
            self.save()
            return chain_data.get('chain_id')

        # RETURN SELF
        if self.data_provider==data_provider:
            return self.data_id
        # RETURN FROM FETCHED DATA
        elif self.combined(priority=self.data_provider).get(data_provider) and not kwargs.get('skip_cache'):
            return self.combined(priority=self.data_provider).get(data_provider)
        # # RETURN FROM SAVED DATA
        # elif self.data.get(data_provider) and not kwargs.get('skip_cache'):
        #     if self.fetched[data_provider]:
        #         self.fetched[data_provider].update({data_provider:self.data.get(data_provider)})
        #     else:
        #         self.fetched[data_provider] = {data_provider:self.data.get(data_provider)}
        #     return self.data.get(data_provider)
        # USE FACTUAL CROSSWALK
        elif data_provider=='factual' and not kwargs.get('skip_crosswalk'):
            # [{u'url': u'https://www.foursquare.com/venue/4cbcafab035d236aebebe64e', u'namespace_id': u'4cbcafab035d236aebebe64e', u'namespace': u'foursquare', u'factual_id': u'13e1b2f6-e492-4c60-b379-c231aaa83ff6'}]
            try:
                response = client_factual.crosswalk().filters({'namespace':self.data_provider,'namespace_id':self.data_id}).data()[0]
                crossed  = {'factual':response.get('factual_id')}
            except Exception, e:
                self.logger.exception("factual crosswalk error: %s" % e)
        # USE SEARCH 
        if not crossed:
            crossed = self.fetch(data_provider=data_provider, name=name, phone=phone, latitude=latitude, longitude=longitude, skip_cache=kwargs.get('skip_cache'))
        # ADD CURRENT DATA TO CROSSED IN ORDER TO UPDATE ALL ON SAVE
        if crossed:            
            crossed.update({self.data_provider:self.data_id})
            if self.fetched.get(data_provider):
                self.fetched[data_provider].update(crossed)
            else:
                self.fetched[data_provider] = crossed
            self.save()
            return crossed.get(data_provider)

    def combined(self, priority=None):
        """
        Combine all of the fetched payloads giving priority to one 
        """
        if priority is None:
            priority = 'foursquare'
        priority_data = self.clean(self.fetched.get(priority) or {}) 
        data = {}
        data.update(self.local)
        for dp, d in self.fetched.items():
            for k,v in d.items():
                data.update({k:v})
        data.update(priority_data)

        return self.clean(data)

    def recurse(self, data_dict, key_list=None):
        """
        Crawl redis through each of the data origins and get the 
            complete set of data_provider, data_id pairs
        """
        if key_list is None:
            key_list= []
        keys = [place_key % x for x in data_dict.items()]
        for key in keys:
            data_dict.update(redis_server.hgetall(key))
        if data_dict.keys()!=key_list:
            self.recurse(data_dict, key_list=data_dict.keys())
        return data_dict


    def get_cache_db(self, data_provider, data_id):
        data = {}
        dp = data_provider
        if dp == 'google_old':
            dp = 'google'
        elif dp == 'factual':
            dp = 'factual_v3'
        entity = self.get_entity(data_provider=dp,data_id=data_id)
        if entity:
            data = self.format(entity)
            data.update({data_provider:data_id})
            self.logger.debug('fetch: from mysql')
        return data

    def get_cache(self, data_provider=None, data_id=None):
        """
        Get data cached in this data origin
        Walk all other data origins found and get their keys 
        Return the full set
        """
        data = {}
        if data_provider is None:
            data_provider = self.data_provider
        if data_id is None:
            data_id = self.data_id
        # RETURN FROM DB?
        data = self.clean(redis_server.hgetall(place_key % (data_provider, data_id)))
        if data:
            self.logger.debug('fetch: from redis')
        else:
            data = self.get_cache_db(data_provider, data_id)
        return self.clean(data, filters=allowed_data)
    
    @property
    def crosswalk_map(self):
        """
        Properly return all data origins for this place even with missing data (e.g.)
            foursquare:id : {'foursquare', 'factual'}
            factual:id    : {'factual', 'google'}
            google:id     : {'google'}
        """
        return self.clean(self.combined(),filters=data_providers)
    

    def clean(self, data_dict, filters=None):
        """
        Used to avoid clobbering data with empty values
        """
        if filters is not None:
            return dict((k, v) for k, v in data_dict.iteritems() if v and v!='None' and k in filters)
        return     dict((k, v) for k, v in data_dict.iteritems() if v and v!='None')

    def _phone(self, place):
        phone = ''
        if isinstance(place,googleplaces.Place):
            if hasattr(place, 'international_phone_number'):
                phone = re.sub("[^0-9]","",place.international_phone_number)
        elif place.get('phone'):
            phone = place.get('phone')
        elif place.get('tel'):
            phone = re.sub("[^0-9]","",place.get('tel'))
        elif place.get('contact') and place.get('contact').get('phone'):
            phone = re.sub("[^0-9]","", place.get('contact').get('phone'))
        if phone and len(phone)==11:
            phone = phone[1:]
        return phone

    def _name(self, place):
        try:
            name = place.name
        except:
            name = place.get('name')
        return name

    def _latitude(self, place):
        latitude = ''
        if isinstance(place,googleplaces.Place):
            latitude = place.geo_location.get('lat')
        elif 'location' in place.keys():
            location = place.get('location')
            latitude = location.get('lat')
        else:
            latitude = place.get('latitude')
        return latitude

    def _longitude(self, place):
        longitude = ''
        if isinstance(place,googleplaces.Place):
            longitude = place.geo_location.get('lng')
        elif 'location' in place.keys():
            location = place.get('location')
            longitude = location.get('lng')
        else:
            longitude = place.get('longitude')
        return longitude

    def format(self, place):
        """
        [
        'foursquare', 'factual', 'google', 'google_old', 
        'name', 'phone', 'latitude', 'longitude',
            'category', 'category_id', 
            'chain_id', 'chain_name'
        ]
        """
        data = {}
        # REQUIRED FOR ALL
        name      = self._name(place)
        phone     = self._phone(place)
        latitude  = self._latitude(place)
        longitude = self._longitude(place)

        # GOOGLE/GOOGLE OLD 
        if isinstance(place, googleplaces.Place):
            data.update({
                'google'    :place.place_id,
                'google_old':place.id
                })
        # WE DON'T KNOW IF IT IS FOURSQUARE/FACTUAL/ OR JUST DICT WITH DATA
        else:
            # ALLOW ANY ALLOWED DATA FROM PLACE TO GET THROUGH
            data.update(self.clean(place, filters=allowed_data))

            # FACTUAL
            if place.get('factual_id'):
                data.update({'factual':place.get('factual_id')})

            # FOURSQUARE
            if place.get('id') and place.get('categories'):
                data.update({'foursquare':place.get('id')})

            if place.get('foursquare'):
                data.update({'foursquare':place.get('foursquare')})

            # FOURSQUARE PRIMARY CATEGORY
            if place.get('categories'):
                try:
                    category_name, category_id =map(lambda x: (x.get('name'), x.get('id')), filter(lambda x: x.get('primary'),place.get('categories')))[0]
                    data.update({'category_id':category_id})
                    data.update({'category_name':slugify(category_name)})
                except IndexError:
                    pass
            # CHAIN DATA
            if place.get('chain_name'):
                data.update({'chain_name':slugify(place.get('chain_name'))})
            if place.get('chain_id'):
                data.update({'chain_id':place.get('chain_id')})
        data.update({'name':name, 'phone':phone,'latitude':latitude,'longitude':longitude})
        return self.clean(data, filters=allowed_data)

        
    def get_chain(self, skip_cache=None):
        """
        Will crosswalk (if necessary) to factual and request chain 
            place = Place(data_provider='foursquare', data_id='50f9c6bfe4b04cffe7833b30', skip_cache=True)
            place.get_chain()
        """
        if self.combined().get('chain_id'):
            return self.combined().get('chain_id')
        self.crosswalk('factual', skip_cache=skip_cache, get_chain=True)
        return self.combined().get('chain_id')
    
    def get_related_by_chain(self, chain_id=None):
        places = []
        place_objects = []
        if chain_id is None:
            chain_id = self.get_chain()
        if chain_id:
            filters = {"$and":[{"chain_id":{"$eq":chain_id}}]}
            offset  = 0 
            limit   = 50
            number_results = 1
            # FOURSQUARE
            while offset < number_results:
                self.logger.debug('%s, %s' % (offset, number_results))
                try:
                    response = client_factual.table('places').filters(filters).include_count(True).offset(offset).limit(limit)
                    places+=response.data()
                    number_results = int(response.total_row_count())
                except APIException, e:
                    self.logger.error(e)
                    offset = number_results
                offset+=limit
        # FETCH FACTUAL PLACES AND CROSSWALK THEM BACK TO FOURSQUARE
        for place in places:
            data_to_save = self.clean(self.combined(), filters=data_providers)
            data_to_save.update(place)
            new_place = Place(data=data_to_save)
            new_place.save()
            place_objects.append(new_place)
        
        return place_objects

    def get_entity(self, data_provider=None, data_id=None):
        entity = None
        if data_provider is None and data_id is None:
            items = self.crosswalk_map.items()
        else:
            items = [(data_provider, data_id)]
        # LOOPS
        for data_provider, data_id in items:
            if data_provider == 'google_old':
                data_provider = 'google'
            elif data_provider == 'factual':
                data_provider = 'factual_v3'
        self.entity = entity
        return self.entity        



