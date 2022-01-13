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
pub struct Flow {
    pub from_id: NodeId, 
    pub into_id: NodeId, 
    pub notes: String, 
    pub share: f32
}
