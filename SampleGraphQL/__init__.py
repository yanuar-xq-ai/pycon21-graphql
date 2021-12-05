import logging

import azure.functions as func

from SampleGraphQL.controllers.graphql import GraphQL


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    query = req.params.get('query')
    if not query:
        try:
            req_body = req.get_json()
            logging.info(req_body)
        except ValueError as e:
            logging.exception(e)
        else:
            query = req_body.get('query')

    try:
        results = GraphQL().query(query)
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse(
             "Internal Server Error", status_code=500
        )

    if results:
        return func.HttpResponse(f"{results}")
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )
