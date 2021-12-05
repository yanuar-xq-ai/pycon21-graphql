import json
import graphene
import uuid
from datetime import datetime, timezone

from graphene.types.scalars import String
from SampleGraphQL.models.memo import DatabaseConnection

# Type DBItem
class Memo(graphene.ObjectType):
    id = graphene.String(required=True)
    topic = graphene.String()
    message = graphene.String()
    tags = graphene.List(String)
    ts = graphene.Int()
    datetime = graphene.types.datetime.DateTime()

    def resolve_datetime(self, info):
        return datetime.fromtimestamp(self._ts)


# Type Query
class Query(graphene.ObjectType):
    hello = graphene.String(argument=graphene.String(default_value="world."))    
    memo = graphene.Field(Memo, id=graphene.String())
    memos = graphene.List(Memo)

    def resolve_hello(self, info, argument):
        return f'Hello {argument}'

    def resolve_memo(self, info, id):
        args = {'id': id}
        results = DatabaseConnection().read_item(args)
        if len(results) > 0:
            item = Memo.__new__(Memo)
            item.__dict__.update(results[0])
            return item
        else:
            return {}

    def resolve_memos(self, info):
        results = []
        for item in DatabaseConnection().read_items():
            i = Memo.__new__(Memo)
            i.__dict__.update(item)
            results.append(i)
        return results


# input MemoInput
class MemoInput(graphene.InputObjectType):
    id = graphene.String(required=True)
    topic = graphene.String()
    message = graphene.String()
    tags = graphene.List(String)


# Create item
class CreateItem(graphene.Mutation):
    class Arguments:
        item = MemoInput(required=True)
    Output = Memo

    def mutate(self, info, item):
        results = DatabaseConnection().upsert_item(item)
        i = Memo.__new__(Memo)
        i.__dict__.update(results)
        return i


# Delete item
class DeleteItem(graphene.Mutation):
    class Arguments:
        item = MemoInput(required=True)
    Output = Memo

    def mutate(self, info, item):
        results = DatabaseConnection().delete_item(item)
        if len(results) > 0:
            i = Memo.__new__(Memo)
            i.__dict__.update(results[0])
            return i
        return None


# Upsert item
class UpsertItem(graphene.Mutation):
    class Arguments:
        item = MemoInput(required=True)
    Output = Memo

    def mutate(self, info, item):
        results = DatabaseConnection().upsert_item(item)
        i = Memo.__new__(Memo)
        i.__dict__.update(results)
        return i


# Random item
class RandomItem(graphene.Mutation):
    Output = Memo

    def mutate(self, info):
        item = {
            'id': str(uuid.uuid4()).replace('-', ''),
            'topic': 'Auto Generated',
            'message': 'Sample message',
            'tags': ['sample', 'auto message']
        }
        results = DatabaseConnection().upsert_item(item)
        i = Memo.__new__(Memo)
        i.__dict__.update(results)
        return i


# type Mutation
class Mutation(graphene.ObjectType):
    create = CreateItem.Field()
    delete = DeleteItem.Field()
    upsert = UpsertItem.Field()
    random = RandomItem.Field()
    sample = graphene.String()    

    def resolve_sample(self, info):
        item = {
            'id': str(uuid.uuid4()).replace('-', ''),
            'topic': 'Auto Generated',
            'message': 'Sample message',
            'tags': ['sample', 'auto message']
        }
        results = DatabaseConnection().upsert_item(item)
        i = Memo.__new__(Memo)
        i.__dict__.update(results)
        return i


class GraphQL:
    def __init__(self):
        self.schema = graphene.Schema(
            query=Query,
            mutation=Mutation
        )

    def query(self, query):
        results = self.schema.execute(query)
        return json.dumps(results.data)

    def queryWithContext(self, query, context):
        results = self.schema.execute(query, context=context)
        return results
