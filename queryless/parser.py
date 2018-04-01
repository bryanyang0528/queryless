from __future__ import print_function, division
import logging
import yaml

logger = logging.getLogger(__name__)


class YamlParser(object):

    def __init__(self):
        pass

    @staticmethod
    def reader(input_path: str) -> dict:
        with open(input_path, 'r') as f:
            try:
                parsed_yaml = yaml.load(f)
                return parsed_yaml
            except yaml.YAMLError as e:
                logger.error(e)


class BasicParser(YamlParser):

    def __init__(self, path: str):
        self._doc = self.reader(path)

    @property
    def doc(self) -> dict:
        return self._doc

    @property
    def api_version(self) -> str:
        return self._doc.get('apiVersion')

    @property
    def kind(self) -> str:
        return self._doc.get('kind')

    @property
    def metadata(self) -> dict:
        return self._doc.get('metadata')

    @property
    def spec(self) -> dict:
        return self._doc.get('spec')


