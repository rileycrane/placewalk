```
  _____  _                 __          __   _ _             
 |  __ \| |                \ \        / /  | | |            
 | |__) | | __ _  ___ ___   \ \  /\  / /_ _| | | __
 |  ___/| |/ _` |/ __/ _ \   \ \/  \/ / _` | | |/ /
 | |    | | (_| | (_|  __/    \  /\  / (_| | |   <
 |_|    |_|\__,_|\___\___|     \/  \/ \__,_|_|_|\_\
                                                            
 Library for fetching place data, crosswalking, mapping
```                                                            

## Concepts

This library contains three classes
* Place: represents a place
* Fetcher: allows for quick fetching of data from cache or cloud
* Mapper: enables bulk mapping of data

This library allows you to 
* fetch data for places
  * by data_provider, data_id pair
    * from redis cache
    * from mysql cache
    * from foursquare by place id search 
    * from google by place id search 
    * from factual by place id search 
  * by attributes
    * from foursquare by place attrs search 
    * from google by place attrs search 
    * from factual by place attrs search 

The following data keys are stored in redis as a result of using this library
    
    crosswalk:place:foursquare:<id>
    crosswalk:place:factual:<id>   
    crosswalk:place:google:<id>    
    crosswalk:place:google_old:<id>
    example data stored
    ```
    {'category_id': '4bf58dd8d48988d16c941735',
     'category_name': 'meal_takeaway,restaurant,food,establishment',
     'foursquare': '4c3ea5160e0d0f4745e8157f',
     'google': 'ChIJWVWmlEchYYYRlWPMkf5gRvI',
     'google_old': 'f84e893c2754a35ce5ed25c5f8047854e7b52af2',
     'latitude': '27.530142',
     'longitude': '-99.468666',
     'name': 'Whataburger'}
    ```

    crosswalk:places:foursquare     : set of all foursquare ids 
    crosswalk:places:factual        : set of all factual ids 
    crosswalk:places:google         : set of all google ids 
    crosswalk:places:google_old     : set of all old google ids 
    crosswalk:places:<category_id>  : set of all places sharing that category id

    crosswalk:approved_messages:<foursquare_id>          : [chat_ids...]
    crosswalk:related_messages:<foursquare_id>           : [chat_ids...]
    crosswalk:approved_messages:<foursquare_category_id> : [chat_ids...]
    crosswalk:approved_messages:<factual_chain_id>       : [chat_ids...]

    crosswalk:category:<category_id>   : category-name
    crosswalk:category:<category-name> : category_id
    crosswalk:categories: set of all category ids and slugified names

    crosswalk:chain:factual:<chain_id> : [<factual_id>, <factual_id>, <factual_id>]
    crosswalk:chains: set of all factual chain ids 

## Usage

Create a virtualenv for the project.
```
pip install virtualenvwrapper
mkvirtualenv placewalk
workon placewalk
```
Install the dependencies.
```
pip install -r requirements.txt
```


Import library
```
from placewalk import *
```

### Examples
```
from placewalk import *
place = Place(data_provider='google', phone='2122549100')
place.combined() # show data
place.crosswalk('foursquare')
foursquare_id = place.combined().get('foursquare')
```


### Fetch individual places

Get data from foursquare (calling a second time will fetch it from cache. override using ``skip_cache=True``)
```
place = Place(data_provider='foursquare', data_id='4cbcafab035d236aebebe64e')
```

Get data from google
```
place = Place(data_provider='google',     data_id='ChIJPb7KLYdZwokRIgG4nSNM6qg')
```

Get data from factual
```
place = Place(data_provider='factual',    data_id='13e1b2f6-e492-4c60-b379-c231aaa83ff6')
```

Get data from google by search
```
place = Place(data_provider='google',     name='Hotel Chantelle', phone='2122549100')
```

Get data from foursquare by search
```
place = Place(data_provider='foursquare', name='Hotel Chantelle', phone='2122549100')
```

Get data from factual by search
```
place = Place(data_provider='factual', name='Hotel Chantelle', phone='2122549100')
```


### Crosswalk data
Get data from foursquare and then walk it to google and factual
```
place = Place(data_provider='foursquare', name='Hotel Chantelle', phone='2122549100')
place.crosswalk('google')
place.crosswalk('factual')
```

### Useful methods
Get list of known crosswalk ids
```
place.crosswalk_map
```

Get all data from all data sources using 'factual' as the priority source of truth
```
place.combined(priority='factual')
```

Get chain ID from factual
```
place.get_chain()
```

Get all factual places for this place based on possible chain_id
```
place.get_related_by_chain()
```



### Check coverage

How many chains?
```
redis_server.scard(chains_key)
```

How many messages found (by chain) for related places?
```
len(redis_server.keys('crosswalk:related_messages:*'))
```

How many foursquare place ids have been mapped by chain?
```
len(redis_server.smembers(mapped_key % "chains"))
```

Which chain ids have been found?
```
redis_server.smembers(chains_key)
```

