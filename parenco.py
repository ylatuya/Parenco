# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4
#
# Copyright (C) 2011 Andoni Morales

# This file may be distributed and/or modified under the terms of
# the GNU General Public License version 2 as published by
# the Free Software Foundation.
# This file is distributed without any warranty; without even the implied
# warranty of merchantability or fitness for a particular purpose.
# See "LICENSE.GPL" in the source distribution for more information.

from Queue import Queue
from threading import Lock, Thread

import gobject
import gst


class Dispatcher(gst.Element):

    __gstdetails__ = ('dispatcher', 'Dispatcher',
                      'Round robin dispatcher',
                      'Andoni Morales')

    _sinkpadtemplate = gst.PadTemplate("sink",
                                        gst.PAD_SINK,
                                        gst.PAD_ALWAYS,
                                        gst.caps_new_any())

    _srcpadtemplate = gst.PadTemplate("src_%d",
                                        gst.PAD_SRC,
                                        gst.PAD_REQUEST,
                                        gst.caps_new_any())

    __gsttemplates__ = (_sinkpadtemplate, _srcpadtemplate)

    def __init__(self):
        gst.Element.__init__(self)

        self.sinkpad = gst.Pad(self._sinkpadtemplate, "sink")
        self.sinkpad.set_chain_function(self.chainfunc)
        self.sinkpad.set_event_function(self.eventfunc)
        self.add_pad(self.sinkpad)

        self.lock = Lock()
        # Protected with lock
        self.pads = []

    def do_request_new_pad (self, tpl, name):
        if tpl != self._srcpadtemplate:
            return None
        pad = gst.Pad(self._srcpadtemplate, 'src_%d' % (len(self.pads)-1))
        self.add_src_pad(pad)
        return pad

    def add_src_pad(self, pad):
        self.lock.acquire()
        self.pads.append(pad)
        self.lock.release()
        self.add_pad(pad)

    def push_buffer(self, buf):
        self.lock.acquire()
        pad = self.pads[0]
        print "Pushing buf to pad %s\n" % pad.get_name()
        ret = pad.push_buffer(buf)
        self.lock.release()
        return ret

    def push_event(self, event):
        self.lock.acquire()
        pad = self.pads[0]
        print "Pushing event to pad %s\n" % pad.get_name()
        ret = pad.push_event(event)
        self.lock.release()
        return ret

    def rotate_pad(self):
        self.lock.acquire()
        self.pads.insert(0, self.pads.pop())
        self.lock.release()

    def eventfunc (self, pad, event):
        if event.get_structure().get_name() != 'GstForceKeyUnit':
            # Forward events to all source pads
            ret = True
            self.lock.acquire()
            for spad in self.pads:
                if not spad.push_event(event):
                    ret = False
            self.lock.release()
            return ret

        # GstForceKeyUnit
        #self.push_event(event)
        self.rotate_pad()
        self.push_event(event)
        return True

    def chainfunc(self, pad, buf):
        return self.push_buffer(buf)


class Agregator(gst.Element):

    __gstdetails__ = ('agregator', 'Agregator',
                      'Agregator',
                      'Andoni Morales')

    _sinkpadtemplate = gst.PadTemplate("sink_%d",
                                        gst.PAD_SINK,
                                        gst.PAD_REQUEST,
                                        gst.caps_new_any())

    _srcpadtemplate = gst.PadTemplate("src",
                                        gst.PAD_SRC,
                                        gst.PAD_ALWAYS,
                                        gst.caps_new_any())

    __gsttemplates__ = (_sinkpadtemplate, _srcpadtemplate)

    def __init__(self):
        gst.Element.__init__(self)

        self.srcpad = gst.Pad(self._srcpadtemplate, "src")
        self.add_pad(self.srcpad)

        self.queue = Queue()
        self.pads = {}

        self.t = Thread(target=self._stream)
        self.t.daemon = True
        self.t.start()

    def do_request_new_pad (self, tpl, name):
        if tpl != self._sinkpadtemplate:
            return None
        pad = gst.Pad(self._sinkpadtemplate, 'sink_%d' % (len(self.pads)))
        self.add_sink_pad(pad)
        return pad

    def add_sink_pad(self, pad):
        self.pads[pad] = []
        pad.set_chain_function(self.chainfunc)
        pad.set_event_function(self.eventfunc)
        self.add_pad(pad)

    def _stream(self):
        while True:
            self.srcpad.push(self.queue.get())

    def eventfunc (self, pad, event):
        if event.get_structure().get_name() != 'GstForceKeyUnit':
            return True
        for i in range(len(self.pads[pad])):
            self.queue.put(self.pads[pad].pop(0))
        return True

    def chainfunc(self, pad, buffer):
        print "adding buffer to %s" % pad.get_name()
        self.pads[pad].append(buffer)
        return gst.FLOW_OK

gobject.type_register(Dispatcher)
gst.element_register(Dispatcher, "dispatcher", gst.RANK_MARGINAL)
gobject.type_register(Agregator)
gst.element_register(Agregator, "agregator", gst.RANK_MARGINAL)

