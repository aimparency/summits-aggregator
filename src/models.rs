use diesel::Queryable;
use uuid::Uuid; 

pub type NodeId = Uuid;

#[derive(Queryable)]
pub struct Node {
    pub id: NodeId, 
    pub title: String, 
    pub notes: String, 
}

#[derive(Queryable)]
pub struct Connection {
    pub from_node: NodeId, 
    pub to_node: NodeId, 
    pub notes: String, 
    pub flow: f32
}
