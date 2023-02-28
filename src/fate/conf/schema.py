import schema

from fate.util.datastructure import collection


class ConfSchema(schema.Schema):
    """Configuration validation and cleaning.

    Extends `schema.Schema` to support the `ProxyCollection` base used
    by Fate configuration.

    """
    def validate(self, data, **kwargs):
        target = data.__collection__ if isinstance(data, collection.ProxyCollection) else data

        clean = super().validate(target, **kwargs)

        if target is data:
            # we're done
            return clean

        # re-cast
        return type(data)(clean)
