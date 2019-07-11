 /*
  * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

 package com.hazelcast.internal.partition.client;

 import com.hazelcast.client.impl.protocol.ClientMessage;
 import com.hazelcast.client.impl.protocol.codec.AddressCodec;
 import com.hazelcast.client.impl.protocol.codec.ClientMessageType;
 import com.hazelcast.client.impl.protocol.util.ParameterUtil;
 import com.hazelcast.cp.internal.util.Tuple2;
 import com.hazelcast.nio.Address;
 import com.hazelcast.nio.Bits;

 import java.util.AbstractMap;
 import java.util.ArrayList;
 import java.util.Collection;
 import java.util.List;
 import java.util.Map;
 import java.util.Map.Entry;

 /**
  * @since 1.0
  * update 1.5
  */
 @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
 public final class ClientGetPartitionTableCodec {

     public static final ClientMessageType REQUEST_TYPE = ClientMessageType.CLIENT_GETPARTITIONS;
     public static final int RESPONSE_TYPE = 108;

     //************************ REQUEST *************************//

     public static class RequestParameters {
         public static final ClientMessageType TYPE = REQUEST_TYPE;

         /**
          * @since 1.0
          */
         public static int calculateDataSize() {
             int dataSize = ClientMessage.HEADER_SIZE;
             return dataSize;
         }
     }

     /**
      * @since 1.0
      */
     public static ClientMessage encodeRequest() {
         final int requiredDataSize = RequestParameters.calculateDataSize();
         ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
         clientMessage.setMessageType(REQUEST_TYPE.id());
         clientMessage.setRetryable(false);
         clientMessage.setAcquiresResource(false);
         clientMessage.setOperationName("Client.getPartitions");
         clientMessage.updateFrameLength();
         return clientMessage;
     }

     public static RequestParameters decodeRequest(ClientMessage clientMessage) {
         final RequestParameters parameters = new RequestParameters();
         return parameters;
     }

     //************************ RESPONSE *************************//

     public static class ResponseParameters {
         /**
          * @since 1.0
          */
         public List<Map.Entry<Address, List<Tuple2<Integer, Integer>>>> partitions;
         /**
          * @since 1.5
          */
         public boolean partitionStateVersionExist = false;
         public int partitionStateVersion;

         /**
          * @since 1.0
          */
         public static int calculateDataSize(Collection<Entry<Address, List<Tuple2<Integer, Integer>>>> partitions) {
             int dataSize = ClientMessage.HEADER_SIZE;
             dataSize += Bits.INT_SIZE_IN_BYTES;
             for (Map.Entry<Address, List<Tuple2<Integer, Integer>>> partitions_item : partitions) {
                 Address partitions_itemKey = partitions_item.getKey();
                 List<Tuple2<Integer, Integer>> partitions_itemVal = partitions_item.getValue();
                 dataSize += AddressCodec.calculateDataSize(partitions_itemKey);
                 dataSize += Bits.INT_SIZE_IN_BYTES;
                 for (Tuple2<Integer, Integer> partitions_itemVal_item : partitions_itemVal) {
                     dataSize += ParameterUtil.calculateDataSize(partitions_itemVal_item.element1);
                     dataSize += ParameterUtil.calculateDataSize(partitions_itemVal_item.element2);
                 }
             }
             return dataSize;
         }

         /**
          * @since 1.5
          */
         public static int calculateDataSize(Collection<Entry<Address, List<Tuple2<Integer, Integer>>>> partitions, int partitionStateVersion) {
             int dataSize = ClientMessage.HEADER_SIZE;
             dataSize += Bits.INT_SIZE_IN_BYTES;
             for (Entry<Address, List<Tuple2<Integer, Integer>>> partitions_item : partitions) {
                 Address partitions_itemKey = partitions_item.getKey();
                 List<Tuple2<Integer, Integer>> partitions_itemVal = partitions_item.getValue();
                 dataSize += AddressCodec.calculateDataSize(partitions_itemKey);
                 dataSize += Bits.INT_SIZE_IN_BYTES;
                 for (Tuple2<Integer, Integer> partitions_itemVal_item : partitions_itemVal) {
                     dataSize += ParameterUtil.calculateDataSize(partitions_itemVal_item.element1);
                     dataSize += ParameterUtil.calculateDataSize(partitions_itemVal_item.element2);
                 }
             }
             dataSize += Bits.INT_SIZE_IN_BYTES;
             return dataSize;
         }
     }

     /**
      * @since 1.0
      */
     public static ClientMessage encodeResponse(Collection<Map.Entry<Address, List<Tuple2<Integer, Integer>>>> partitions) {
         final int requiredDataSize = ResponseParameters.calculateDataSize(partitions);
         ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
         clientMessage.setMessageType(RESPONSE_TYPE);
         clientMessage.set(partitions.size());
         for (Map.Entry<Address, List<Tuple2<Integer, Integer>>> partitions_item : partitions) {
             Address partitions_itemKey = partitions_item.getKey();
             List<Tuple2<Integer, Integer>> partitions_itemVal = partitions_item.getValue();
             AddressCodec.encode(partitions_itemKey, clientMessage);
             clientMessage.set(partitions_itemVal.size());
             for (Tuple2<Integer, Integer> partitions_itemVal_item : partitions_itemVal) {
                 clientMessage.set(partitions_itemVal_item.element1);
                 clientMessage.set(partitions_itemVal_item.element2);
             }
         }
         clientMessage.updateFrameLength();
         return clientMessage;

     }

     /**
      * @since 1.5
      */
     public static ClientMessage encodeResponse(Collection<Entry<Address, List<Tuple2<Integer, Integer>>>> partitions, int partitionStateVersion) {
         final int requiredDataSize = ResponseParameters.calculateDataSize(partitions, partitionStateVersion);
         ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
         clientMessage.setMessageType(RESPONSE_TYPE);
         clientMessage.set(partitions.size());
         for (Entry<Address, List<Tuple2<Integer, Integer>>> partitions_item : partitions) {
             Address partitions_itemKey = partitions_item.getKey();
             List<Tuple2<Integer, Integer>> partitions_itemVal = partitions_item.getValue();
             AddressCodec.encode(partitions_itemKey, clientMessage);
             clientMessage.set(partitions_itemVal.size());
             for (Tuple2<Integer, Integer> partitions_itemVal_item : partitions_itemVal) {
                 clientMessage.set(partitions_itemVal_item.element1);
                 clientMessage.set(partitions_itemVal_item.element2);
             }
         }
         clientMessage.set(partitionStateVersion);
         clientMessage.updateFrameLength();
         return clientMessage;

     }

     public static ResponseParameters decodeResponse(ClientMessage clientMessage) {
         ResponseParameters parameters = new ResponseParameters();
         List<Map.Entry<Address, List<Tuple2<Integer, Integer>>>> partitions;
         int partitions_size = clientMessage.getInt();
         partitions = new ArrayList<>(partitions_size);
         for (int partitions_index = 0; partitions_index < partitions_size; partitions_index++) {
             Map.Entry<Address, List<Tuple2<Integer, Integer>>> partitions_item;
             Address partitions_item_key;
             List<Tuple2<Integer, Integer>> partitions_item_val;
             partitions_item_key = AddressCodec.decode(clientMessage);
             int partitions_item_val_size = clientMessage.getInt();
             partitions_item_val = new ArrayList<>(partitions_item_val_size);
             for (int partitions_item_val_index = 0;
                  partitions_item_val_index < partitions_item_val_size; partitions_item_val_index++) {
                 Tuple2<Integer, Integer> partitions_item_val_item;
                 partitions_item_val_item = Tuple2.of(clientMessage.getInt(), clientMessage.getInt());
                 partitions_item_val.add(partitions_item_val_item);
             }
             partitions_item = new AbstractMap.SimpleEntry<>(partitions_item_key, partitions_item_val);
             partitions.add(partitions_item);
         }
         parameters.partitions = partitions;

         if (clientMessage.isComplete()) {
             return parameters;
         }
         int partitionStateVersion = 0;
         partitionStateVersion = clientMessage.getInt();
         parameters.partitionStateVersion = partitionStateVersion;
         parameters.partitionStateVersionExist = true;
         return parameters;
     }

 }
