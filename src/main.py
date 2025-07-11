import core, rpc, p2p, ws

broadcaster = ws.WSBroadcaster('0.0.0.0')
p2p = p2p.P2PNode('0.0.0.0', 1069)
core = core.Core(p2p, broadcaster)
rpc = rpc.RPC(core, 6934)

rpc.run(host='0.0.0.0', port=8080, threaded=True)