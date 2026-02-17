//! PubSub Radix Tree Node: Topic routing data structure

use std::collections::{HashMap, HashSet};
use crate::brokers::pub_sub::types::ClientId;
use crate::brokers::pub_sub::retained::RetainedMessage;

pub(crate) struct Node {
    // Exact match children: "kitchen" -> Node
    pub(crate) children: HashMap<String, Node>,
    
    // Wildcard '+' child: matches any single token at this level
    // Note: Used for routing messages TO subscribers who used '+'
    pub(crate) plus_child: Option<Box<Node>>,
    
    // Wildcard '#' child: matches everything remaining
    // Note: Used for routing messages TO subscribers who used '#'
    pub(crate) hash_child: Option<Box<Node>>,
    /// Only stores ClientIds. Sender resolution happens via shared DashMap at publish time.
    pub(crate) subscribers: HashSet<ClientId>,
    pub(crate) retained: Option<RetainedMessage>,
}

impl Node {
    pub(crate) fn new() -> Self {
        Self {
            children: HashMap::new(),
            plus_child: None,
            hash_child: None,
            subscribers: HashSet::new(),
            retained: None,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
            && self.children.is_empty()
            && self.plus_child.is_none()
            && self.hash_child.is_none()
            && self.retained.is_none()
    }
}
