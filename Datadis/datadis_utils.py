import ast
import os
from tempfile import NamedTemporaryFile

from neo4j import GraphDatabase

from utils import decrypt


def generate_input_tsv(config, data):
    with NamedTemporaryFile(delete=False, suffix=".tsv", mode='w') as file:
        for i in data:
            enc_dict = ast.literal_eval(i['password'])
            password = decrypt(enc_dict, os.getenv(config['encript_pass']['environment']))
            tsv_str = f"{i['username']}\t{password}\t{i['user']}\t{i['namespace']}\n"
            file.write(tsv_str)
    return file.name


def get_users(neo4j):
    driver = GraphDatabase.driver(**neo4j)
    with driver.session() as session:
        users = session.run(
            f"""Match (n:DatadisSource)<-[:ns0__hasSource]->(o:ns0__Organization) 
            CALL{{With o Match (o)<-[*]-(d:ns0__Organization) WHERE NOT (d)<-[:ns0__hasSubOrganization]-() return d}}
             return n.username AS username, n.password AS password, 
             d.ns0__userId AS user, split(d.uri,"#")[0]+'#'AS namespace
            """).data()
    return users
