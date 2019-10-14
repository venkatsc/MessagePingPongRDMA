package com.message;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import com.ibm.disni.verbs.IbvWC;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.util.LinkedList;

public class RdmaSendReceiveUtil {

    public static void postSendReq(RdmaActiveEndpoint endpoint, int workReqId) throws IOException {

        if (endpoint instanceof RdmaShuffleEndpoint) {
            RdmaShuffleEndpoint clientEndpoint = (RdmaShuffleEndpoint) endpoint;
//            System.out.println("posting srv send wr_id " + workReqId);
            LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
            IbvSge sendSGE = new IbvSge();
            sendSGE.setAddr(((DirectBuffer) clientEndpoint.getSendBuffer()).address());
            sendSGE.setLength(clientEndpoint.getSendBuffer().capacity());
            sendSGE.setLkey(clientEndpoint.getMR().getLkey());
            sges.add(sendSGE);

            // Create send Work Request (WR)
            IbvSendWR sendWR = new IbvSendWR();
            sendWR.setWr_id(workReqId);
            sendWR.setSg_list(sges);
//			if (finish){
//				sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
//			}else {
            sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
//			}
            sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);

            LinkedList<IbvSendWR> sendWRs = new LinkedList<>();
            sendWRs.add(sendWR);
            clientEndpoint.postSend(sendWRs).execute().free();
        }
//        } else if (endpoint instanceof RdmaShuffleClientEndpoint) {
//            RdmaShuffleClientEndpoint clientEndpoint = (RdmaShuffleClientEndpoint) endpoint;
////            System.out.println("posting cli send wr_id " + workReqId);
//            LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
//            IbvSge sendSGE = new IbvSge();
//            sendSGE.setAddr(((DirectBuffer) clientEndpoint.getSendBuffer()).address());
//            sendSGE.setLength(clientEndpoint.getSendBuffer().capacity());
//            sendSGE.setLkey(clientEndpoint.getMR().getLkey());
//            sges.add(sendSGE);
//            // Create send Work Request (WR)
//            IbvSendWR sendWR = new IbvSendWR();
//            sendWR.setWr_id(workReqId);
//            sendWR.setSg_list(sges);
////			if (finish) {
////				sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
////			} else {
//            sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
////			}
//            sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
//
//            LinkedList<IbvSendWR> sendWRs = new LinkedList<>();
//            sendWRs.add(sendWR);
//            clientEndpoint.postSend(sendWRs).execute().free();
//        }
    }

    public static void postReceiveReq(RdmaActiveEndpoint endpoint, int workReqId) throws IOException {

        if (endpoint instanceof RdmaShuffleEndpoint) {
//            System.out.println("posting srv recv wr_id " + workReqId);
            RdmaShuffleEndpoint clientEndpoint = (RdmaShuffleEndpoint) endpoint;
            LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
            IbvSge recvSGE = new IbvSge();
            recvSGE.setAddr(((DirectBuffer) clientEndpoint.getReceiveBuffer()).address());
            recvSGE.setLength(clientEndpoint.getReceiveBuffer().capacity());
            recvSGE.setLkey(clientEndpoint.getMR().getLkey());
            sges.add(recvSGE);

            IbvRecvWR recvWR = new IbvRecvWR();
            recvWR.setWr_id(workReqId);
            recvWR.setSg_list(sges);

            LinkedList<IbvRecvWR> recvWRs = new LinkedList<>();
            recvWRs.add(recvWR);
            endpoint.postRecv(recvWRs).execute().free();
        }
//        } else if (endpoint instanceof RdmaShuffleClientEndpoint) {
////            System.out.println("posting cli recv wr_id " + workReqId);
//            RdmaShuffleClientEndpoint clientEndpoint = (RdmaShuffleClientEndpoint) endpoint;
//            LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
//            IbvSge recvSGE = new IbvSge();
//            recvSGE.setAddr(((DirectBuffer) clientEndpoint.getReceiveBuffer()).address());
//            recvSGE.setLength(clientEndpoint.getReceiveBuffer().capacity());
//            recvSGE.setLkey(clientEndpoint.getMR().getLkey());
//            sges.add(recvSGE);
//
//            IbvRecvWR recvWR = new IbvRecvWR();
//            recvWR.setWr_id(workReqId);
//            recvWR.setSg_list(sges);
//
//            LinkedList<IbvRecvWR> recvWRs = new LinkedList<>();
//            recvWRs.add(recvWR);
//            endpoint.postRecv(recvWRs).execute().free();
//        }
    }
}

