#!/usr/bin/env python3
import csv
import pandas as pd
from urllib.parse import quote
import requests
from io import StringIO 
import rdflib 
from rdflib import Graph, BNode, Literal, Namespace
# See https://rdflib.readthedocs.io/en/latest/_modules/rdflib/namespace.html
from rdflib.namespace import QB, RDF, XSD,SKOS
from urllib.parse import quote

NS = Namespace("https://example.org/ontology#")
NSR = Namespace("https://example.org/resources/")
OKRESY = Namespace("https://example.org/okresy/")
KRAJE = Namespace("https://example.org/kraje/")
# We use custom Namespace here as the generated is limited in content
# https://rdflib.readthedocs.io/en/stable/_modules/rdflib/namespace/_RDFS.html
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
DCT = Namespace("http://purl.org/dc/terms/")
SDMX_SUBJECT = Namespace("http://purl.org/linked-data/sdmx/2009/subject#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")


def main(outp_path='./'):
    df = get_data()
    data_cube = as_data_cube(df.to_dict(orient='records'))
    with open(f'{outp_path}population.ttl','wb') as f:
        f.write(data_cube.serialize(format="ttl",encoding='utf-8'))
    run_constraint_checks(data_cube)

def get_data():
    with requests.get('https://www.czso.cz/documents/10180/184344914/130141-22data2021.csv', stream=True) as r:
        cols = {'vuzemi_cis':int,'vuzemi_kod':int,'vuk':str,'hodnota':int}
        df = pd.read_csv(StringIO(r.content.decode('utf-8')),usecols=cols.keys(),converters=cols)
    

    with requests.get('https://apl.czso.cz/iSMS/do_cis_export?kodcis=109&typdat=0&cisjaz=203&format=2&separator=%2C', stream=True) as r:
        cols = {'kodrso':int,'chodnota':str}
        okresy_code_mapper = pd.read_csv(StringIO(r.content.decode('utf-8')),usecols=cols.keys(),converters=cols)
        okresy_code_mapper = okresy_code_mapper.set_index('kodrso').squeeze().to_dict()

    with requests.get('https://apl.czso.cz/iSMS/do_cis_export?kodcis=100&typdat=0&cisjaz=203&format=2&separator=%2C', stream=True) as r:
        cols = {'chodnota':int,'cznuts':str}
        kraje_code_mapper = pd.read_csv(StringIO(r.content.decode('utf-8')),usecols=cols.keys(),converters=cols)
        kraje_code_mapper = kraje_code_mapper.set_index('chodnota').squeeze().to_dict()
    
    with requests.get('https://apl.czso.cz/iSMS/do_cis_export?kodcis=100&typdat=1&cisvaz=101_132&cisjaz=203&format=2&separator=%2C', stream=True) as r:
        cols = {'chodnota1':int,'chodnota2':int}
        okresy_to_kraje = pd.read_csv(StringIO(r.content.decode('utf-8')),usecols=cols.keys(),converters=cols)
        okresy_to_kraje = okresy_to_kraje.set_index('chodnota2').squeeze().to_dict()
     
    df = df[(df['vuk']=='DEM0004') & (df['vuzemi_cis']==101)]
    df['krajCode'] = df.replace({"vuzemi_kod": okresy_to_kraje}).replace({"vuzemi_kod": kraje_code_mapper})['vuzemi_kod']
    df = df.replace({"vuzemi_kod": okresy_code_mapper})
    df = df.rename(columns={"vuzemi_kod": "okresCode",'hodnota':'population'})
    return df

def as_data_cube(data):
    result = rdflib.Graph()
    dimensions = create_dimensions(result)
    measures = create_measure(result)
    structure = create_structure(result, dimensions, measures)
    dataset = create_dataset(result, structure)
    create_observations(result, dataset, data)
    return result


def create_dimensions(collector: Graph):

    okres = NS.okres
    collector.add((okres, RDF.type, RDFS.Property))
    collector.add((okres, RDF.type, QB.DimensionProperty))
    collector.add((okres, RDFS.label, Literal("Okres", lang="cs")))
    collector.add((okres, RDFS.label, Literal("County", lang="en")))
    collector.add((okres, SKOS.prefLabel, Literal("County")))
    collector.add((okres, RDFS.range, XSD.string))

    kraj = NS.kraj
    collector.add((kraj, RDF.type, RDFS.Property))
    collector.add((kraj, RDF.type, QB.DimensionProperty))
    collector.add((kraj, RDFS.label, Literal("Kraj", lang="cs")))
    collector.add((kraj, RDFS.label, Literal("County", lang="en")))
    collector.add((kraj, SKOS.prefLabel, Literal("County")))
    collector.add((kraj, RDFS.range, XSD.string))



    return [okres, kraj]


def create_measure(collector: Graph):
    
    mean_population = NS.mean_population
    collector.add( ( mean_population, RDF.type, RDFS.Property) )
    
    collector.add( ( mean_population, RDF.type, QB.MeasureProperty ) )
    collector.add( ( mean_population, RDFS.label, Literal("Stredni stav obyvatel", lang="cs") ) )
    collector.add( ( mean_population, RDFS.label, Literal("Mean population", lang="en") ) )
    collector.add( ( mean_population, SKOS.prefLabel, Literal("Mean population")))
    collector.add( ( mean_population, RDFS.range, XSD.integer ) )
    collector.add( (mean_population,  RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

    return [mean_population]


def create_structure(collector: Graph, dimensions, measures):

    structure = NS.structure
    collector.add((structure, RDF.type, QB.DataStructureDefinition))

    for dimension in dimensions:
        component = BNode()
        collector.add((structure, QB.component, component))
        collector.add((component, QB.dimension, dimension))

    for measure in measures:
        component = BNode()
        collector.add((structure, QB.component, component))
        collector.add((component, QB.measure, measure))
        collector.add((component, QB.componentProperty, measure))
        collector.add((component, QB.measureDimension, measure))
    return structure


def create_dataset(collector: Graph, structure):

    dataset = NSR.dataCubeInstance

    collector.add((dataset, RDF.type, QB.DataSet))
    collector.add((dataset, RDFS.label, Literal("Pocet obyvatel okresu", lang="cs")))
    collector.add((dataset, RDFS.label, Literal("County population", lang="en")))
    collector.add((dataset, QB.structure, structure))

    collector.add((dataset, DCT.description, Literal("County population in Czechia")))
    collector.add((dataset, DCT.comment, Literal("Population in counties of Czechia")))
    collector.add((dataset, DCT.issued, Literal("2023-3-12", datatype=XSD.date)))
    collector.add((dataset, DCT.publisher, Literal("Tomas Zasadil")))

    collector.add((dataset, DCT.subject, NS.Population))
    collector.add((dataset, DCT.subject, NS.RegionalStatictics))
    collector.add((dataset, DCT.subject, NS.Czechia))
   

    return dataset


def create_observations(collector: Graph, dataset, data):
    for index, row in enumerate(data):
        resource = NSR["observation-" + str(index).zfill(3)]
        create_observation(collector, dataset, resource, row)

def create_observation(collector: Graph, dataset, resource, data):
    collector.add((resource, RDF.type, QB.Observation))
    collector.add((resource, QB.dataSet, dataset))
    collector.add((resource, NS.okres, OKRESY[escape(data["okresCode"])]))
    collector.add((resource, NS.kraj, KRAJE[escape(data["krajCode"])]))
    collector.add((resource, NS.mean_population, Literal(data["population"], datatype=XSD.integer)))

def escape(value):
    return quote(value.replace(' ','_'))

def run_constraint_checks(graph: Graph):
    import constrains as constrains
    graph.namespace_manager.bind('qb',QB)
    graph.namespace_manager.bind('skos',SKOS)
    for query in constrains.integrity_queries:
        result = graph.query(query)
        assert not result.askAnswer, 'The datacube is not well formed'
    print('All tests have passed => the datacube is well formed')

if __name__ == "__main__":
    main()
