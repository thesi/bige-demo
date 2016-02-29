grammar ConditionalExp;
                 
expression 		: logicalExpr EOF;

logicalExpr		: logicalExpr AND logicalExpr  	# LogicalExpressionAnd 
				| logicalExpr OR logicalExpr  	# LogicalExpressionOr 
				| LPAREN logicalExpr RPAREN		# LogicalExpressionInParen
				| primary 						# LogicalPrimary
				| BOOLEAN						# LogicalEntity
				;				

primary			: operand atomicCompare operand 	# PrimaryAtomicCompare
				| operand 'IN' operand 				# PrimarySetCompare	
				| ID '(' parameterList? ')'			# PrimaryFunctionCall
				;

atomicCompare	: GT
            	| GE
            	| LT
            	| LE
            	| EQ
            	| NE
            	;
			
operand		: 'Subject.' fieldName 							# OperandSubjectAttribute
			| 'Resource.' fieldName 						# OperandResourceAttribute
			| 'Environment.' fieldName 						# OperandEnvironmentAttribute
			| (ID '.')? ID '.' fieldName					# OperandDataPath
			| numericEntity 								# OperandConstant
			;
			
fieldName		:  ID (INDEX ? ('.' ID))*;
parameterList	:  operand (',' operand)*;	
numericEntity 	: DECIMAL 
				| STRING
				;

AND : 'AND' ;
OR  : 'OR' ;

INDEX			: '.'[0-9]+;
FIELD			: '.'[a-zA-Z_][a-zA-Z_0-9]+;
DECIMAL 		: '-'?[0-9]+('.'[0-9]+)? ;
STRING 			: '"' (~('\\'|'"'))* '"';
BOOLEAN			: 'true'|    'false';
ID  			: [a-zA-Z_][a-zA-Z_0-9]+ ;
WS  			: (' '|'\t')+ {skip();} ;

TRUE  : 'true' ;
FALSE : 'false' ;

MULT  : '*' ;
DIV   : '/' ;
PLUS  : '+' ;
MINUS : '-' ;

GT : '>' ;
GE : '>=' ;
LT : '<' ;
LE : '<=' ;
EQ : '=' ;
NE : '!=' ;

LPAREN : '(' ;
RPAREN : ')' ;
    