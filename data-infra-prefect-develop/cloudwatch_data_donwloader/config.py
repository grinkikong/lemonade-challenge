NAMESPACES = {
	'AWS/GameCast': {
		'metrics': [
			'ActiveCapacity',
			'IdleCapacity',
			'MemoryUtilization',
			'CPUUtilization',
			'FrameCaptureRate',
			'AudioCaptureRate',
			'RoundTripTime',
			'TerminatedStreamSessions',
			'ErroredStreamSessions',
			'SessionLength'
		],
		'period': 300
	},
	'Another/Namespace': {
		'metrics': [
			'Metric1',
			'Metric2',
			'Metric3'
		],
		'period': 120  # seconds
	}
}

STATISTICS = ['Average', 'Sum', 'Maximum', 'Minimum', 'SampleCount']
KAFKA_TOPIC = 'data.cloudwatch.metrics'
KAFKA_EVENT_NAME = 'cloudwatch_metric_fecthed'