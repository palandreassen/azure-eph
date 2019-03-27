import json
from flask import Flask, request, Response
from azure.eventhub import EventHubClient, Offset, subscribe
from ast import literal_eval
import logging
import cherrypy
import os

app = Flask(__name__)

address = os.environ.get('address')

# Access tokens for event hub namespace, from Azure portal for namespace
user = os.environ.get('user')
key = os.environ.get('key')
consumergroup = os.environ.get('consumergroup')

PARTITION = "0"

# address = "amqps://simentest.servicebus.windows.net/hafslund3"
# consumergroup = "$default"
# user = "RootManageSharedAccessKey"
# key = "TdFr9BVV7ACjSCzSFO2XDz0RN/URDopKNVOPPmID0v4="

if not address:
    logging.error("No event hub address supplied")


@app.route('/', methods=['GET', 'POST'])
def get():
    if request.args.get('since') is None:
        since = -1
    else:
        since = request.args.get('since')
    client = EventHubClient(address, debug=False, username=user, password=key)
    try:
        receiver = client.add_receiver(consumergroup, PARTITION, prefetch=100000, offset=Offset(since), keep_alive=7200)
        client.run()
        print(client.get_eventhub_info())
        # if err is not None:
        #     logging.error("Client run: " + err)
    except KeyboardInterrupt:
        pass

    def generate():
        yield '['
        index = 0
        batched_events = receiver.receive(max_batch_size=100000, timeout=5000)
        for event_data in batched_events:
            if index > 0:
                yield ','
            # parse x:
            last_sn = event_data.sequence_number
            data = str(event_data.message)
            output_entity = literal_eval(data)
            output_entity.update({"_updated": str(last_sn)})
            yield json.dumps(output_entity)
            index = index + 1
        yield ']'
    return Response(generate(), mimetype='application/json')


if __name__ == '__main__':
    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.socket_port': 5000,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
