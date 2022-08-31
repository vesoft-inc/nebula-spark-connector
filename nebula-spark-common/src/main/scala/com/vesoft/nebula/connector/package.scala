package com.vesoft.nebula

import com.vesoft.nebula.connector.writer.NebulaExecutor

package object connector {

  type Address        = (String, Int)
  type NebulaType     = Int
  type Prop           = List[Any]
  type PropertyNames  = List[String]
  type PropertyValues = List[Any]

  type VertexID           = Long
  type VertexIDSlice      = String
  type NebulaGraphxVertex = (VertexID, PropertyValues)
  type NebulaGraphxEdge   = org.apache.spark.graphx.Edge[(EdgeRank, Prop)]
  type EdgeRank           = Long

  case class NebulaVertex(vertexIDSlice: VertexIDSlice, values: PropertyValues) {
    def propertyValues = values.mkString(", ")

    override def toString: String = {
      s"Vertex ID: ${vertexIDSlice}, Values: ${values.mkString(", ")}"
    }
  }

  case class NebulaVertices(propNames: PropertyNames,
                            values: List[NebulaVertex],
                            policy: Option[KeyPolicy.Value]) {

    def propertyNames: String = NebulaExecutor.escapePropName(propNames).mkString(",")

    override def toString: String = {
      s"Vertices: " +
        s"Property Names: ${propNames.mkString(", ")}" +
        s"Vertex Values: ${values.mkString(", ")} " +
        s"with policy: ${policy}"
    }
  }

  case class NebulaEdge(source: VertexIDSlice,
                        target: VertexIDSlice,
                        rank: Option[EdgeRank],
                        values: PropertyValues) {
    def propertyValues: String = values.mkString(", ")

    override def toString: String = {
      s"Edge: ${source}->${target}@${rank} values: ${propertyValues}"
    }
  }

  case class NebulaEdges(propNames: PropertyNames,
                         values: List[NebulaEdge],
                         sourcePolicy: Option[KeyPolicy.Value],
                         targetPolicy: Option[KeyPolicy.Value]) {
    def propertyNames: String = NebulaExecutor.escapePropName(propNames).mkString(",")
    def getSourcePolicy       = sourcePolicy
    def getTargetPolicy       = targetPolicy

    override def toString: String = {
      "Edges:" +
        s" Property Names: ${propNames.mkString(", ")}" +
        s" with source policy ${sourcePolicy}" +
        s" with target policy ${targetPolicy}"
    }
  }
}
