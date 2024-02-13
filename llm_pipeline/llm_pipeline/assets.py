from dagster import asset


@asset
def raw_documents():
    pass

@asset
def documents(raw_documents):
    pass

@asset
def vectorstore(documents):
    pass
