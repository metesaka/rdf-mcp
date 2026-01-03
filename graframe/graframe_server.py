from fastmcp import FastMCP
from rdflib import Graph, URIRef, Literal, Namespace,  RDFS
from rdflib.term import Variable
from typing import List, Optional
import sys
import logging
from matcher import ConceptMatcher
import json
from query import Query

logging.basicConfig(level=logging.INFO)
mcp = FastMCP("GraFrameBrickServer")

S223 = Namespace("http://data.ashrae.org/standard223#")
ontology = Graph(store="Oxigraph").parse("https://brickschema.org/schema/1.4/Brick.ttl")

BRICK = Namespace("https://brickschema.org/schema/Brick#")
with open("graframe/lexicon.json", "r") as f:
    lexicon = json.load(f)

matcher = ConceptMatcher(lexicon)
query = Query()

def as_brick_uri(term: str) -> URIRef:
    # Accept "Chiller", "brick:Chiller", or full URI
    if term.startswith("http://") or term.startswith("https://"):
        return URIRef(term)
    if term.startswith("brick:"):
        term = term.split(":", 1)[1]
    return URIRef(BRICK + term)

@mcp.tool()
def expand_abbreviation(abbreviation: str, k: int) -> list[str]:
    """
    Expand an abbreviation to its full form using the Brick ontology and ConceptMatcher
    Top k results are returned (increase k for ambiguous abbreviations)
    Ordered by closest to furthest
    return type:
    [ uri1, uri2, ... ]

    """
    closest_matches_matcher = matcher.match(abbreviation,restrict_kinds={"class"}, top_k=k, min_score=0.25)
    ret_dict = {}
    for match in closest_matches_matcher:
        ret_dict[match.label] = ontology.compute_qname(match.uri)[2]
    if closest_matches_matcher:
        logging.info(f"closest match to {abbreviation} is {closest_matches_matcher[0].label}")
    return list(ret_dict.values())

@mcp.tool()
def find_close_uriref(search_string: str, k: int) -> list[str]:
    """
    Find potential classes or predicates using the Brick ontology and ConceptMatcher
    Top k results are returned (increase k for ambiguous abbreviations)
    Ordered by closest to furthest
    return type:
    [ uri1, uri2, ... ]

    """
    closest_matches_matcher = matcher.match(search_string,restrict_kinds={"class"}, top_k=k, min_score=0.25)
    ret_dict = {}
    for match in closest_matches_matcher:
        ret_dict[match.label] = ontology.compute_qname(match.uri)[2]
    if closest_matches_matcher:
        logging.info(f"closest match to {search_string} is {closest_matches_matcher[0].label}")
    return list(ret_dict.values())

@mcp.tool()
def get_terms() -> list[str]:
    """Get all terms in the Brick ontology graph"""
    query = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    PREFIX s223: <http://data.ashrae.org/standard223#>
    SELECT ?class WHERE {
        { ?class a owl:Class }
        UNION
        { ?class a rdfs:Class }
        FILTER NOT EXISTS { ?class owl:deprecated true }
        FILTER NOT EXISTS { ?class brick:aliasOf ?alias }
    }"""
    results = ontology.query(query)
    # return [str(row[0]).split('#')[-1] for row in results]
    r = [str(row[0]).split("#")[-1] for row in results]
    return r

@mcp.tool()
def get_properties() -> list[str]:
    """Get all properties in the Brick ontology graph"""
    query = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX s223: <http://data.ashrae.org/standard223#>
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?prop WHERE {
        { ?prop rdfs:subPropertyOf ?property }
        UNION
        { ?prop a owl:ObjectProperty }
        UNION
        { ?prop a owl:DataProperty }
    }"""
    results = ontology.query(query)
    # return [str(row[0]).split('#')[-1] for row in results]
    r = [str(row[0]).split("#")[-1] for row in results]
    return r

@mcp.tool()
def get_subclasses(parent_class: str) -> list[str]:
    """Get all classes that inherit from a specific parent class in the Brick ontology"""
    query = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    SELECT DISTINCT ?subclass WHERE {
        ?subclass rdfs:subClassOf* ?parent .
        ?subclass a owl:Class .
        FILTER NOT EXISTS { ?subclass owl:deprecated true }
        FILTER (?subclass != ?parent)
    }"""
    results = ontology.query(query, initBindings={"parent": BRICK[parent_class]})
    return [str(row[0]).split("#")[-1] for row in results]

@mcp.tool()
def get_brick_tags(term: str) -> list[str]:
    """Get all tags associated with a Brick class or term"""
    query = """
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX tag: <https://brickschema.org/schema/BrickTag#>
    SELECT DISTINCT ?tag WHERE {
        ?term brick:hasAssociatedTag ?tag .
    }"""
    results = ontology.query(query, initBindings={"term": BRICK[term]})
    return [str(row[0]).split("#")[-1] for row in results]

@mcp.tool()
def get_possible_properties(class_: str) -> list[tuple[str, str]]:
    """Returns pairs of possible (property, object type) for a given brick class"""
    query = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX s223: <http://data.ashrae.org/standard223#>
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?path ?type WHERE {
        ?from brick:aliasOf?/rdfs:subClassOf* ?fromp .
        { ?shape brick:aliasOf?/sh:targetClass ?fromp }
        UNION
        { ?fromp a sh:NodeShape . BIND(?fromp as ?shape) }
        ?shape sh:property ?prop .
        ?prop sh:path ?path .
         FILTER (!isBlank(?path))
        OPTIONAL { { ?prop sh:node ?type } UNION { ?prop sh:class ?type } }
    }
    """
    res = list(ontology.query(query, initBindings={"from": BRICK[class_]}).bindings)
    print(res, file=sys.stderr)
    path_object_pairs = set([(r[Variable("path")], r[Variable("type")]) for r in res])
    return list(path_object_pairs)

@mcp.tool()
def get_definition_brick(class_: str) -> str:
    """Get the definition of cyber-physical concepts from the Brick ontology."""
    return ontology.cbd(BRICK[class_]).serialize(format="turtle")

@mcp.tool()
def find_entity(class_: str, alias: str) -> dict:
    """Add a new entity node to the query and set it as the current pointer.
        Provide a valid Brick Class URI and an alias for the node.
        alias must be unique within the query and will be used to reference this node in future query operations.

        Example:
            q = aq.query().find_entity(
                _class="urn:nawi-water-ontology#Valve",
                alias="valve",
            )

        This creates a new QueryNode and makes it the default pointer.
        Edits the global query object and returns its dictionary representation
        
        """
    global query
    query = query.find_entity(as_brick_uri(class_), alias=alias)
    return query.to_dict()

@mcp.tool()
def find_related_entities(
        _class:str, alias:str, _from:str, hops:int=3, predicates:list[str]=None, multi_hop_predicates:bool=False
    ) -> dict:
    """Add a related entity node, connected from an existing node.

        Semantics:
        - `_from` is an alias of an existing node.
        - Adds a new node of type `_class` with the given alias.
        - Adds an edge from the `_from` node to the new node, with a hop limit.

        Example:
            q1 = aq.query().find_entity(_class=Valve, alias="valve")
            q1 = q1.find_related(_class=Pump, alias="related_pump", _from="valve")

        Edits the global query object and returns its dictionary representation

        """
    global query
    query = query.find_related(
        _class=as_brick_uri(_class),
        alias=alias,
        _from=_from,
        hops=hops,
        predicates=predicates,
        multi_hop_predicates=multi_hop_predicates
    )
    return query.to_dict()

@mcp.tool()
def find_points(_from:str, alias:str) -> dict:
    """
    Find sensor or setpoint (data generating points, nodes with an external reference) related to a specific term
    If from = None, use current pointer
    If from is "*", search from all nodes in the query
    Edits the global query object and returns its dictionary representation
    """
    global query
    query = query.find_data(_from=_from, alias=alias)
    return query.to_dict()

@mcp.tool()
def get_sparql_query() -> str:
    """Get the SPARQL query representation of the current query object."""
    global query
    return query.to_sparql()

@mcp.resource("rdf://describe/{term}")
def get_definition(term: str) -> str:
    """Get the definition of cyber-physical concepts like sensors from the Brick ontology."""
    return ontology.cbd(as_brick_uri(term)).serialize(format="turtle")

@mcp.tool()
def reset_query() -> str:
    global query
    query = Query()
    return "ok"




def _get_terms() -> list[str]:
    """Get all terms in the Brick ontology graph"""
    query = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    PREFIX s223: <http://data.ashrae.org/standard223#>
    SELECT ?class WHERE {
        { ?class a owl:Class }
        UNION
        { ?class a rdfs:Class }
        FILTER NOT EXISTS { ?class owl:deprecated true }
        FILTER NOT EXISTS { ?class brick:aliasOf ?alias }
    }"""
    results = ontology.query(query)
    # return [str(row[0]).split('#')[-1] for row in results]
    r = [str(row[0]).split("#")[-1] for row in results]
    return r

# build a dictionary of all classes in the Brick ontology
def build_class_dict() -> dict[str, str]:
    """Build a dictionary of all classes in the Brick ontology"""
    class_dict = {}
    for term in _get_terms():
        class_uri = BRICK[term]
        label = ontology.value(subject=class_uri, predicate=RDFS.label)
        if label:
            label = str(label)
        else:
            label = str(term).split("#")[-1]
        class_dict[label] = term
    return class_dict

# Initialize CLASS_DICT as an empty dict so it can be patched in tests
CLASS_DICT = {}
CLASS_DICT = build_class_dict()
logging.info("mpc ready")




