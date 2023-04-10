#!/usr/bin/env python3
import csv
import pandas as pd
import html
from urllib.parse import quote

import rdflib 
from rdflib import Graph, BNode, Literal, Namespace
# See https://rdflib.readthedocs.io/en/latest/_modules/rdflib/namespace.html
from rdflib.namespace import QB, RDF, XSD,SKOS,PROV, FOAF

E = Namespace("https://example.org/")
NS = Namespace("https://example.org/ontology/")
NSR = Namespace("https://example.org/resources/")
# We use custom Namespace here as the generated is limited in content
# https://rdflib.readthedocs.io/en/stable/_modules/rdflib/namespace/_RDFS.html
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
DCT = Namespace("http://purl.org/dc/terms/")
OKRESY = Namespace("https://example.org/okresy/")
KRAJE = Namespace("https://example.org/kraje/")
OBORY = Namespace("https://example.org/obor-pece/")
SDMX_SUBJECT = Namespace("http://purl.org/linked-data/sdmx/2009/subject#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")


def add_provenance(collector: Graph):

    collector.add(( NS.bob, RDF.type, FOAF.Person))
    collector.add(( NS.bob, RDF.type, PROV.Agent))
    collector.add(( NS.bob,  FOAF.givenName ,  Literal("Bob",datatype=XSD.string)))
 
    
    
    collector.add(( NS.datasetCreationActivity, RDF.type, PROV.Activity))
    collector.add(( NS.datasetCreationActivity, PROV.qualifiedAssociation, NS.datasetCreationAssociation))       
    
    collector.add(( NS.datasetCreationAssociation, RDF.type, PROV.Association))
    collector.add(( NS.datasetCreationAssociation, PROV.agent, NS.bob))
    collector.add(( NS.datasetCreationAssociation, NS.database, NS.crappyDatabaseSystem))
    
    collector.add(( NS.programmer, RDF.type, PROV.Role))

    collector.add(( NS.crappyDatabaseSystem, RDF.type, PROV.SoftwareAgent))
    collector.add(( NS.crappyDatabaseSystem, RDF.type, PROV.Agent))
    collector.add(( NS.crappyDatabaseSystem, FOAF.name, Literal("Excel",datatype=XSD.string)))
    

