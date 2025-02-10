const fs = require('fs');

function convertMarkdownToMDX(markdownContent) {
	// Step 1: Protect code blocks first
	const codeBlocks = [];
	let protectedContent = markdownContent.replace(/```[\s\S]*?```/g, (match) => {
		codeBlocks.push(match);
		return `__CODE_BLOCK_${codeBlocks.length - 1}__`;
	});

	// Step 2: Protect inline code
	const inlineCode = [];
	protectedContent = protectedContent.replace(/`[^`]+`/g, (match) => {
		inlineCode.push(match);
		return `__INLINE_CODE_${inlineCode.length - 1}__`;
	});

	// Step 3: Escape JSX-like syntax outside of code blocks
	protectedContent = protectedContent
	// Handle all comparison operators
	.replace(/(?<!`|\\)([<>]=?|==|===|!=|!==|<=|>=)/g, '{"$1"}')
	// Handle HTML-like tags that aren't actually JSX components
	.replace(/(?<!`|\\)<(?!\/?[A-Z])[^>]+>/g, (match) => {
		return `{'${match}'}`;
	});

	// Step 4: Restore code blocks
	codeBlocks.forEach((block, index) => {
		protectedContent = protectedContent.replace(
			`__CODE_BLOCK_${index}__`,
			block
		);
	});

	// Step 5: Restore inline code
	inlineCode.forEach((code, index) => {
		protectedContent = protectedContent.replace(
			`__INLINE_CODE_${index}__`,
			code
		);
	});

	return protectedContent;
}

// Example usage
const processFile = (inputPath, outputPath) => {
	try {
		const markdown = fs.readFileSync(inputPath, 'utf8');
		const mdx = convertMarkdownToMDX(markdown);
		fs.writeFileSync(outputPath, mdx);
		console.log(`Successfully converted ${inputPath} to ${outputPath}`);
	} catch (error) {
		console.error('Error processing file:', error);
	}
};

// CLI interface
if (require.main === module) {
	const [,, input, output] = process.argv;
	if (!input || !output) {
		console.log('Usage: node markdown-to-mdx.js input.md output.mdx');
		process.exit(1);
	}
	processFile(input, output);
}

module.exports = { convertMarkdownToMDX };
