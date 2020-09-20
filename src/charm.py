#!/usr/bin/python3
import os
import sys
import copy
import subprocess
import json

from ops.charm import CharmBase

from ops.main import main


import logging
import socket
import json

from ops.framework import (
    EventBase,
    EventSource,
    Object,
    ObjectEvents,
    StoredState,
)


logger = logging.getLogger()


class SlurmctldPeerAvailableEvent(EventBase):
    """Emmited on the relation_changed event."""


class PeerRelationEvents(ObjectEvents):
    """Peer Relation Events"""
    slurmctld_peer_available = EventSource(SlurmctldPeerAvailableEvent)


class TestingPeerRelation(Object):

    on = PeerRelationEvents()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._charm = charm
        self._relation_name = relation_name

        self.framework.observe(
            self._charm.on[self._relation_name].relation_created,
            self._on_relation_created
        )

        self.framework.observe(
            self._charm.on[self._relation_name].relation_joined,
            self._on_relation_joined
        )

        self.framework.observe(
            self._charm.on[self._relation_name].relation_changed,
            self._on_relation_changed
        )

        self.framework.observe(
            self._charm.on[self._relation_name].relation_departed,
            self._on_relation_departed
        )

        self.framework.observe(
            self._charm.on[self._relation_name].relation_broken,
            self._on_relation_broken
        )

    def _on_relation_created(self, event):
        logger.debug("################ LOGGING RELATION CREATED ####################")

        relation = self.framework.model.get_relation(self._relation_name)

        unit_relation_data = relation.data[self.model.unit]
        unit_relation_data['hostname'] = self._charm.get_hostname()
        unit_relation_data['port'] = self._charm.get_port()

        if self.framework.model.unit.is_leader():
            app_relation_data = relation.data[self.model.app]
            app_relation_data['active_controller'] = self.model.unit.name
            app_relation_data['backup_controller'] = ""
            app_relation_data['standby_controllers'] = json.dumps([])

    def _on_relation_joined(self, event):
        logger.debug("################ LOGGING RELATION JOINED ####################")
        logger.debug(_get_active_peers())

    def _on_relation_changed(self, event):
        logger.debug("################ LOGGING RELATION CHANGED ####################")
        # We only modify the slurmctld controller queue
        # if we are the leaader. As such, we dont need to preform
        # any operations if we are not the leader.
        if self.framework.model.unit.is_leader():
                
            relation = self.framework.model.get_relation(self._relation_name)

            app_relation_data = relation.data[self.model.app]
            unit_relation_data = relation.data[self.model.unit] 

            slurmctld_peers = _get_active_peers()
            slurmctld_peers_tmp = copy.deepcopy(slurmctld_peers)
    
            active_controller = app_relation_data.get('active_controller')
            backup_controller = app_relation_data.get('backup_controller')
    
            standby_controllers = []
    
            try:
                standby_controllers.extend(
                    json.loads(app_relation_data.get('standby_controllers'))
                )
            except json.decoder.JSONDecodeError as e:
                logger.error(e)
                sys.exit(1)

            # Account for the active controller
            # If we are the leader but are not the active controller, then the previous
            # leader or active controller must have died. In this case set the leader as
            # the active controller.
            if active_controller != self.model.unit.name:
                app_relation_data['active_controller'] = self.model.unit.name

            # Account for the backup controller
            #
            # If the backup controller exists in the application relation data
            # then check that it also exists in the slurmctld_peers. If it does
            # exist in the slurmctld peers then remove it from the list of active peers and
            # set the rest of the peers to be standby controllers.
            if backup_controller:
                # Just because the backup_controller exists in the application data doesn't
                # mean that it really exists. Check that the backup_controller that we have in the
                # application data still exists in the list of active units. If the backup_controller
                # isn't in the list of active units then check for standby_controllers > 0 and try to
                # promote a standby to a backup.
                if backup_controller in slurmctld_peers:
                    slurmctld_peers_tmp.remove(backup_controller)
                    app_relation_data['standby_controllers'] = json.dumps(slurmctld_peers_tmp)
                else:
                    if len(slurmctld_peers) > 0:
                        app_relation_data['backup_controller'] = slurmctld_peers_tmp.pop()
                        app_relation_data['standby_controllers'] = json.dumps(slurmctld_peers_tmp)
                    else:
                        app_relation_data['backup_controller'] = ""
                        app_relation_data['standby_controllers'] = json.dumps([])
            else:
                if len(slurmctld_peers) > 0:
                    app_relation_data['backup_controller'] = slurmctld_peers_tmp.pop()
                    app_relation_data['standby_controllers'] = json.dumps(slurmctld_peers_tmp)
                else:
                    app_relation_data['standby_controllers'] = json.dumps([])

            ctxt = {}
            backup_controller = app_relation_data.get('backup_controller')

            if backup_controller:
                for unit in relation.units:
                    logger.debug(f"UNIT NAME : {unit.name}")
                    logger.debug(f"BACKUP CONTROLLER: {backup_controller}")

                    if unit.name == backup_controller:
                        unit_data = relation.data[unit]
                        hostname = unit_data.get("hostname")
                        port = unit_data.get("port")

                        if not (hostname and port):
                            event.defer()
                            return

                        ctxt['backup_controller_ingress_address'] = \
                            unit_data['ingress-address']
                        ctxt['backup_controller_hostname'] = \
                            unit_data['hostname']
                        ctxt['backup_controller_port'] = unit_data['port']
            else:
                ctxt['backup_controller_ingress_address'] = ""
                ctxt['backup_controller_hostname'] = ""
                ctxt['backup_controller_port'] = ""

            ctxt['active_controller_ingress_address'] = \
                unit_relation_data['ingress-address']

            ctxt['active_controller_hostname'] = self._charm.get_hostname()

            ctxt['active_controller_port'] = self._charm.get_port()

            logger.debug(ctxt)

            self._charm.set_slurmctld_info(json.dumps(ctxt))
            self.on.slurmctld_peer_available.emit()

        
    def _on_relation_departed(self, event):
        logger.debug("################ LOGGING RELATION DEPARTED ####################")

    def _on_relation_broken(self, event):
        logger.debug("################ LOGGING RELATION BROKEN ####################")


def _related_units(relid):
    """List of related units."""
    units_cmd_line = ['relation-list', '--format=json', '-r', relid]
    return json.loads(
        subprocess.check_output(units_cmd_line).decode('UTF-8')) or []


def _relation_ids(reltype):
    """List of relation_ids."""
    relid_cmd_line = ['relation-ids', '--format=json', reltype]
    return json.loads(
        subprocess.check_output(relid_cmd_line).decode('UTF-8')) or []


def _get_active_peers():
    """Return the active_units."""
    active_units = []
    for rel_id in _relation_ids('slurmctld-peer'):
        for unit in _related_units(rel_id):
            active_units.append(unit)
    return active_units

class PeererCharm(CharmBase):

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self._stored.set_default(slurmctld_info=str())
        self._stored.set_default(slurmctld_controller_type=str())
        
        self._slurmctld_peer = TestingPeerRelation(self, "slurmctld-peer")
        
        self.framework.observe(self.on.install, self.on_install)
        self.framework.observe(self.on.start, self.on_start)
        self.framework.observe(self.on.leader_elected, self._on_leader_elected)
        self.framework.observe(self._slurmctld_peer.on.slurmctld_peer_available, self.on_slurmctld_peer_available)

    def on_slurmctld_peer_available(self, event):
        logger.debug(self._stored.slurmctld_info)

    def _on_leader_elected(self, event):
        self._slurmctld_peer._on_relation_changed(event)

    def on_install(self, event):
        pass

    def on_start(self, event):
        pass

    def set_slurmctld_info(self, slurmctld_info):
        self._stored.slurmctld_info = slurmctld_info

    def get_hostname(self):
        return socket.gethostname().split(".")[0]

    def get_port(self):
        return "88888"

if __name__ == "__main__":
    main(PeererCharm)
