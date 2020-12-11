from pony import orm
import datetime as dt
import config

db = orm.Database()

class Series(db.Entity):
    ticker = orm.Required(str, unique=True)
    description = orm.Required(str)
    country = orm.Required(str)
    source = orm.Required('Source')
    observation = orm.Set('Observation')
    table = orm.Set('Table')


class Observation(db.Entity):
    date = orm.Required(dt.datetime)
    obs = orm.Required(float)
    series = orm.Required(Series)


class Source(db.Entity):
    name = orm.Required(str, unique=True)
    description = orm.Required(str)
    series = orm.Set(Series)

    
class Table(db.Entity):
    ticker = orm.Required(str, unique=True)
    description =  orm.Required(str, unique=True)
    series = orm.Set(Series)


# bootstrap db:
db.bind(provider=config.DB["provider"], user=config.DB["user"], 
        password=config.DB["password"], host=config.DB["host"],
        port=config.DB["port"], database=config.DB["database"])
db.generate_mapping(create_tables=True)




