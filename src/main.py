import core, rpc, p2p, ws
import pyfiglet, os

os.system("cls" if os.name == "nt" else "clear")
print(pyfiglet.figlet_format("Xylume TestNet", font="doom"))

broadcaster = ws.WSBroadcaster('0.0.0.0')
p2p = p2p.P2PNode('0.0.0.0', 25795)
core = core.Core(p2p, broadcaster)
rpc = rpc.RPC(core, 6934)

rpc.run(host='0.0.0.0', port=25614, threaded=True)