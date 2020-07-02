#! /usr/bin/env python3
from ops.charm import CharmBase

from ops.main import main


import logging
import socket
import json

from ops.framework import (
    Object,
    ObjectEvents,
    StoredState,
)


logger = logging.getLogger()


class PeerRelationEvents(ObjectEvents):
    """Peer Relation Events"""


class TestingPeerRelation(Object):

    on = PeerRelationEvents()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)

        self.hostname = socket.gethostname()

        self.framework.observe(
            charm.on[relation_name].relation_created,
            self._on_relation_created
        )

        self.framework.observe(
            charm.on[relation_name].relation_joined,
            self._on_relation_joined
        )

        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self._on_relation_changed
        )

        self.framework.observe(
            charm.on[relation_name].relation_departed,
            self._on_relation_departed
        )

        self.framework.observe(
            charm.on[relation_name].relation_broken,
            self._on_relation_broken
        )

    def _on_relation_created(self, event):
        logger.debug("################ LOGGING RELATION CREATED ####################")
        event.relation.data[self.model.unit]['hostname'] = self.hostname

    def _on_relation_joined(self, event):
        logger.debug("################ LOGGING RELATION JOINED ####################")

    def _on_relation_changed(self, event):
        logger.debug("################ LOGGING RELATION CHANGED ####################")
        
    def _on_relation_departed(self, event):
        logger.debug("################ LOGGING RELATION DEPARTED ####################")

    def _on_relation_broken(self, event):
        logger.debug("################ LOGGING RELATION BROKEN ####################")


class PeererCharm(CharmBase):

    def __init__(self, *args):
        super().__init__(*args)
        
        self.slurmd_peer = TestingPeerRelation(self, "slurmd")
        
        self.framework.observe(self.on.install, self.on_install)
        self.framework.observe(self.on.start, self.on_start)

    def on_install(self, event):
        pass

    def on_start(self, event):
        pass

if __name__ == "__main__":
    main(PeererCharm)
