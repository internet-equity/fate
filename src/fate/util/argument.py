import argparse


class ChoiceMapping(argparse.Action):
    """Argparse Action to interpret the `choices` argument as a
    mapping of user-specified choices values to the resulting option
    values.

    """
    def __init__(self, *args, choices, **kwargs):
        super().__init__(*args, choices=choices.keys(), **kwargs)
        self.mapping = choices

    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, self.mapping[value])
