module.exports = {
	extends: ['@commitlint/config-conventional'],
	defaultIgnores: false,
	rules: {
		'header-max-length': [2, 'always', 100],
		'body-max-line-length': [2, 'always', Infinity],
		'type-enum': [
			2,
			'always',
			['feat', 'fix', 'docs', 'style', 'refactor', 'test', 'chore', 'revert', 'perf']
		],
		'footer-max-line-length': [2, 'always', Infinity],
		'footer-leading-blank': [0]
	}
};
