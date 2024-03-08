# -*- coding: utf-8 -*-

from twisted.internet.defer import inlineCallbacks

from coinla_spider.databases.getters import get_model_obj
from coinla_spider.databases.models import ModuleConcept, ModuleCurrencyMiddle
from coinla_spider.pipelines.base_pipelines import ConnectionPipeline


class ConceptPipeline(ConnectionPipeline):

    @inlineCallbacks
    def process_item(self, item, spider):
        concept_name = item['concept_name']
        mc_id = yield self.get_mc_id(concept_name)

        for url in item['ccy_urls']:
            ccy_id = yield self._cache.load('CurrencyUrl', url)
            if not ccy_id:
                yield self._cache.save(
                    'Addition', 'Currency', url, command='sadd')
                continue
            yield self.add_new_mcm(mc_id, ccy_id)
        return item

    @inlineCallbacks
    def get_mc_id(self, concept_name):
        mc_id = yield self._cache.load('ModuleConcept', concept_name)
        if mc_id is None:
            filters = {ModuleConcept.mc_name == concept_name}
            attrs = {'mc_name': concept_name}
            mc = get_model_obj(ModuleConcept, filters, attrs)
            mc_id = mc.id
            yield self._cache.save('ModuleConcept', concept_name, mc_id)
        return mc_id

    @inlineCallbacks
    def add_new_mcm(self, mc_id, ccy_id):
        mcm_id = yield self._cache.load(
            'ModuleCurrencyMiddle', '{}-{}'.format(mc_id, ccy_id))
        if mcm_id is None:
            filters = {
                ModuleCurrencyMiddle.mc_id == mc_id,
                ModuleCurrencyMiddle.ccy_id == ccy_id
            }
            attrs = {
                'mc_id': mc_id,
                'ccy_id': ccy_id
            }
            mcm = get_model_obj(ModuleCurrencyMiddle, filters, attrs)
            yield self._cache.save(
                'ModuleCurrencyMiddle', '{}-{}'.format(mc_id, ccy_id), mcm.id)
