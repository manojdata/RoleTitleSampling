import pandas as pd
from zipfile import ZipFile
import json
import os
import argparse


def unpack_inputs_figure_eight(unpack_files_path, figure_eight_output, figure_eight_report_path):

    figure_eight_report = pd.read_csv(figure_eight_report_path, error_bad_lines=False, encoding='utf-8', sep=",")
    figure_eight_annotations = []

    try:
        with ZipFile(figure_eight_output, 'r') as zipObj:
            listOfFileNames = zipObj.namelist()

            for fileName in listOfFileNames:
                if "aggregated" not in fileName:
                    annotation = json.loads(str(zipObj.read(fileName).decode("utf-8")))
                    annotation_id = fileName.split("/")[3].split("_")[0]
                    annotation["seek_metadata"] = get_seek_metadata(annotation_id, figure_eight_report)
                    figure_eight_annotations.append(annotation)
    except :
        raise Exception('error unpacking file')

    if figure_eight_annotations:
        pd.DataFrame(figure_eight_annotations).to_json(unpack_files_path +
                                                       '/figure_eight_results.json', orient='records')


def get_seek_metadata(annotation_id, figure_eight_report):

    seek_metadata_by_annotation = figure_eight_report[figure_eight_report["id"].astype(str) == str(annotation_id)]
    if len(seek_metadata_by_annotation) == 1:
        seek_metadata = {"jobid": str(seek_metadata_by_annotation["jobid"].iloc[0]),
                                       "classid": str(seek_metadata_by_annotation["classid"].iloc[0]),
                                       "subclassid": str(seek_metadata_by_annotation["subclassid"].iloc[0])}
    else:
        seek_metadata = {"jobid": "",
                         "classid": "",
                         "subclassid": ""}
    return seek_metadata


def read_aggregated_data_figure_eight(file_path_aggregated):

    with open(file_path_aggregated) as f:
        figure_eight_aggregate_data = json.load(f)

    return figure_eight_aggregate_data


def get_start_end_multiple_word_labels(span):
    start = span["tokens"][0]['startIdx']
    end = span["tokens"][-1]['endIdx']
    return start, end


def get_start_end_span(span):

    if len(span["tokens"]) > 1:
        start, end = get_start_end_multiple_word_labels(span)

    else:
        start = span["tokens"][0]['startIdx']
        end = span["tokens"][0]['endIdx']

    return start, end


def get_spans(spans):

    spacy_format_spans = []
    for span in spans:
        if span["annotated_by"] == "human":
            start, end = get_start_end_span(span)
            spacy_format_spans.append([start, end, span["classname"]])

    return spacy_format_spans


def get_tokens(tokens):

    spacy_format_tokens = []
    for token in tokens:
        spacy_format_tokens.append({"text": token['text'], "start": token['startIdx'], "end": token['endIdx']})

    return spacy_format_tokens


def transformation_input(annotations):

    spacy_format_input = []

    for annotation in annotations:
        text = annotation["text"]
        seek_metadata = annotation["seek_metadata"]
        spans = annotation["spans"]
        tokens = annotation["tokens"]

        normalized_spans = []
        if spans:
            normalized_spans = get_spans(spans)
        if tokens:
            normalized_tokens = get_tokens(tokens)

        spacy_format_input.append({"text": text, "entities": normalized_spans, "meta": seek_metadata,
                                   "tokens": normalized_tokens})

    return spacy_format_input


def execute_transformation(figure_eight_report_path, figure_eight_zip_path, raw_data_dir):

    unpack_inputs_figure_eight(raw_data_dir, figure_eight_zip_path, figure_eight_report_path)
    file_path_aggregated = raw_data_dir + "/figure_eight_results.json"
    annotations = read_aggregated_data_figure_eight(file_path_aggregated)
    spacy_annotations = transformation_input(annotations)
    pd.DataFrame(spacy_annotations).drop_duplicates(subset=["text"], keep="first")\
        .to_json(raw_data_dir + "/spacy_input_transformed.json", lines=True, orient="records")
    print("Transformation done please see document: " + raw_data_dir + "/spacy_input_transformed.json")


def arg_parser():
    parser = argparse.ArgumentParser(description='Figure eight to spacy')
    parser.add_argument('-r', '--figure_eight_report', required=True, type=str)
    parser.add_argument('-d', '--figure_eight_zip_file', required=True, type=str)
    return parser


if __name__ == '__main__':
    args = arg_parser().parse_args()
    raw_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'data/raw/'))
    pd.options.mode.chained_assignment = None
    figure_eight_zip_path = raw_data_dir + "/" + str(args.figure_eight_zip_file)
    figure_eight_report_path = raw_data_dir + "/" + str(args.figure_eight_report)
    execute_transformation(figure_eight_report_path, figure_eight_zip_path, raw_data_dir)