#!/usr/bin/env python3
import csv
import pandas as pd
from urllib.parse import quote
from html import escape
from provenance import add_provenance

import rdflib 
from rdflib import Graph, BNode, Literal, Namespace
# See https://rdflib.readthedocs.io/en/latest/_modules/rdflib/namespace.html
from rdflib.namespace import QB, RDF, XSD,SKOS, PROV, DCAT,FOAF

NS = Namespace("https://example.org/ontology#")
NSR = Namespace("https://example.org/resources/")
NSD = Namespace("https://example.org/files/")
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
COUNTRIES = Namespace("https://publications.europa.eu/resource/authority/country/")
FILE_TYPES = Namespace("http://publications.europa.eu/resource/authority/file-type/")
UPDATE_FREQ = Namespace("http://publications.europa.eu/resource/authority/frequency/")
EUROVOC = Namespace("http://eurovoc.europa.eu/")


def save_rdf(graph):
    with open(f'population_dataset_info.trig','wb') as f:
        f.write(graph.serialize(format='trig',encoding='utf-8'))
    


def make_graph():
    graph = rdflib.Graph()
    dataset = NSR.populationDataset
    
    me = BNode()
    graph.add((me, RDF.type, FOAF.Person))
    graph.add((me, FOAF.name,Literal("Tomas Zasadil")))

    graph.add((dataset, RDF.type, DCAT.Dataset))
    graph.add((dataset, DCT.title,Literal("Population dataset", lang="en")))
    graph.add((dataset, RDFS.label,Literal("Population dataset", lang="en")))
    graph.add((dataset, DCT.issued,Literal("2023-5-08", datatype=XSD.date)))
    graph.add((dataset, DCT.modified,Literal("2023-5-08", datatype=XSD.date)))
    graph.add((dataset, DCT.publisher, me))
    graph.add((dataset, DCT.creator, me))
    graph.add((dataset, DCT.spatial, COUNTRIES.CZE))
    graph.add((dataset, DCAT.keyword, Literal("czechia", lang="en")))
    graph.add((dataset, DCAT.keyword, Literal("population", lang="en")))
    graph.add((dataset, DCAT.keyword, Literal("region statistics", lang="en")))
    graph.add((dataset, DCT.accrualPeriodicity,UPDATE_FREQ.NEVER))

    graph.add((dataset, DCAT.theme,EUROVOC['5860']))
    graph.add((dataset, DCAT.theme,EUROVOC['3300']))
   
    distribution = NSR.populationDatasetRDF
    graph.add((dataset, DCAT.distribution, distribution))
    graph.add((distribution, RDF.type, DCAT.Distribution))
    graph.add((distribution, DCAT.mediaType,FILE_TYPES.RDF_TRIG))
    graph.add((distribution, DCAT.accessURL,NSD['population.trig']))
    graph.add((distribution, DCT.title,Literal("RDF-trig distribution of the population dataset", lang="en")))
    

    return graph

if __name__ == "__main__":
    graph = make_graph()
    save_rdf(graph)
