#!/usr/bin/env python3
import csv
import pandas as pd



import rdflib 
from rdflib import Graph, BNode, Literal, Namespace
# See https://rdflib.readthedocs.io/en/latest/_modules/rdflib/namespace.html
from rdflib.namespace import QB, RDF, XSD,SKOS

NS = Namespace("https://example.org/ontology#")
NSR = Namespace("https://example.org/resources/")
# We use custom Namespace here as the generated is limited in content
# https://rdflib.readthedocs.io/en/stable/_modules/rdflib/namespace/_RDFS.html
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
DCT = Namespace("http://purl.org/dc/terms/")
SDMX_SUBJECT = Namespace(" http://purl.org/linked-data/sdmx/2009/subject#")
SDMX_CONCEPT = Namespace(" http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace(" http://purl.org/linked-data/sdmx/2009/measure#")


def main():
    required_columns = ['KrajCode','OkresCode','DruhZarizeni']
    df = pd.read_csv('data/zdravotnici.csv',usecols=required_columns)
    df = df.groupby(required_columns).size().reset_index(name ='Count')
    data_cube = as_data_cube(df.to_dict(orient='records'))
    # print(data_cube.serialize(format="ttl"))
    run_constraint_checks(data_cube)
    print("-" * 80)


def load_csv_file_as_object(file_path: str):
    result = []
    with open(file_path, "r") as stream:
        reader = csv.reader(stream)
        header = next(reader)  # Skip header
        for line in reader:
            result.append({key: value for key, value in zip(header, line)})
    return result


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
    collector.add((okres, RDFS.range, SDMX_CONCEPT.refArea))

    kraj = NS.kraj
    collector.add((kraj, RDF.type, RDFS.Property))
    collector.add((kraj, RDF.type, QB.DimensionProperty))
    collector.add((kraj, RDFS.label, Literal("kraj", lang="cs")))
    collector.add((kraj, RDFS.label, Literal("County", lang="en")))
    collector.add((kraj, SKOS.prefLabel, Literal("County")))
    collector.add((kraj, RDFS.range, XSD.string))
    collector.add((kraj, RDFS.range, SDMX_CONCEPT.refArea))

    obor_pece = NS.obor_pece
    collector.add((obor_pece, RDF.type, RDFS.Property))
    collector.add((obor_pece, RDF.type, QB.DimensionProperty))
    collector.add((obor_pece, RDFS.label, Literal("Obor péče", lang="cs")))
    collector.add((obor_pece, RDFS.label, Literal("Field of care", lang="en")))
    collector.add((obor_pece, SKOS.prefLabel, Literal("Field of care")))
    collector.add((obor_pece, RDFS.range, XSD.string))



    return [okres, kraj, obor_pece]


def create_measure(collector: Graph):
    
    number_of_care_providers = NS.number_of_care_providers
    collector.add( ( number_of_care_providers, RDF.type, RDFS.Property) )
    collector.add( ( number_of_care_providers, RDF.type, QB.MeasureProperty ) )
    collector.add( ( number_of_care_providers, RDFS.label, Literal("Počet poskytovatelů péče", lang="cs") ) )
    collector.add( ( number_of_care_providers, RDFS.label, Literal("Number of care providers", lang="en") ) )
    collector.add((number_of_care_providers, SKOS.prefLabel, Literal("Number of care providers")))
    collector.add( ( number_of_care_providers, RDFS.range, XSD.integer ) )
    collector.add( (number_of_care_providers,  RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

    return [number_of_care_providers]


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
    collector.add((dataset, RDFS.label, Literal("Poskytovatelé zdravotních služeb", lang="cs")))
    collector.add((dataset, RDFS.label, Literal("Care Providers", lang="en")))
    collector.add((dataset, QB.structure, structure))

    collector.add((dataset, DCT.description, Literal("Care Providers Czechia")))
    collector.add((dataset, DCT.comment, Literal("Number of different types of care providers in counties of Czechia")))
    collector.add((dataset, DCT.issued, Literal("2023-3-12", datatype=XSD.date)))
    collector.add((dataset, DCT.publisher, Literal("Tomas Zasadil")))

    collector.add((dataset, DCT.subject, NS.Health))
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
    collector.add((resource, NS.okres, Literal(data["OkresCode"])))
    collector.add((resource, NS.kraj, Literal(data["KrajCode"])))
    collector.add((resource, NS.obor_pece, Literal(data["DruhZarizeni"])))
    collector.add((resource, NS.number_of_care_providers, Literal(data["Count"], datatype=XSD.integer)))

def convert_date(value):
    return value.replace(".", "-")

def run_constraint_checks(graph: Graph):
    import constrains
    graph.namespace_manager.bind('qb',QB)
    graph.namespace_manager.bind('skos',SKOS)
    for query in constrains.integrity_queries:
        result = graph.query(query)
        assert not result.askAnswer, 'The datacube is not well formed'
    print('All tests have passed => the datacube is well formed')

if __name__ == "__main__":
    main()
