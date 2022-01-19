from neo4j import GraphDatabase


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
