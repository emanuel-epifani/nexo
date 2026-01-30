pub mod stream_manager;
pub mod topic;
pub mod group;
pub mod message;
pub mod commands;
pub mod persistence;

pub use stream_manager::StreamManager;

#[cfg(test)]
mod tests {
    use crate::brokers::stream::topic::TopicState;
    use bytes::Bytes;

    #[test]
    fn test_topic_state_pure_logic() {
        let mut state = TopicState::new("test_topic".to_string(), 2);
        
        // 1. Publish (Partition 0)
        let (offset, _) = state.publish(0, Bytes::from("msg1"));
        assert_eq!(offset, 0);
        assert_eq!(state.get_high_watermark(0), 1);

        // 2. Publish (Partition 1)
        let (offset, _) = state.publish(1, Bytes::from("msg2"));
        assert_eq!(offset, 0);

        // 3. Read
        let msgs = state.read(0, 0, 10);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, Bytes::from("msg1"));
        
        let msgs = state.read(1, 0, 10);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, Bytes::from("msg2"));
    }

    #[test]
    fn test_read_offset_logic() {
        let mut state = TopicState::new("test".to_string(), 1);
        for i in 0..10 {
            state.publish(0, Bytes::from(format!("msg{}", i)));
        }

        // Read all
        let all = state.read(0, 0, 100);
        assert_eq!(all.len(), 10);
        assert_eq!(all[0].offset, 0);
        assert_eq!(all[9].offset, 9);

        // Read mid
        let mid = state.read(0, 5, 100);
        assert_eq!(mid.len(), 5);
        assert_eq!(mid[0].offset, 5);
        
        // Read future
        let future = state.read(0, 100, 100);
        assert!(future.is_empty());
    }
}
