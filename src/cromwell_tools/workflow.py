import json


class Workflow:

    def __init__(self, wdl, inputs_json, options_json, version='v1'):
        self.wdl = wdl
        self.inputs_json = inputs_json
        self.options_json = options_json
        self.version = version
        self._run_id = None

    @property
    def submission_json(self):
        return {
            'workflowSource': open(self.wdl, 'rb'),
            'workflowInputs': open(self.inputs_json, 'rb'),
            'workflowOptions': open(self.options_json, 'rb')
        }

    @property
    def run_id(self):
        return self._run_id

    @run_id.setter
    def run_id(self, value):
        # todo run some checks that the workflow id is valid
        self._run_id = value
