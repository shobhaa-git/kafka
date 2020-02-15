package kafka.log.remote2

import kafka.cluster.Partition

class RemoteLogManager {



  def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]): Unit = {

  }



}
