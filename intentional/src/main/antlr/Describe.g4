grammar Describe;
@header {
package it.unibo.antlr.gen;
}

describe   : 'with' c 'describe' mc+=id (',' mc+=id)*
              (('for' sc=clause)? ('by' gc+=id (',' gc+=id)*)? | ('by' gc+=id (',' gc+=id)*)? ('for' sc=clause)?)
              ('using' models+=MODEL (',' models+=MODEL)*)? ('size' k=INT)? EOF;

id locals[String name] : ID { $name = $ID.text; };

c: cube=id;

clause     : condition (binary condition)*;

condition
  : attr=ID op=comparator val+=value
  | attr=ID in=IN '(' val+=value (',' val+=value)* ')';

value
  : ID
  | DECIMAL 
  | INT 
  | bool;

comparator
  : GE 
  | LE
  | EQ
  | GT
  | LT;

binary: AND;
bool: TRUE | FALSE;

IN         : 'IN' | 'in';
AND        : 'AND' | 'and'; 
NOT        : 'NOT' | 'not';
TRUE       : 'TRUE' | 'true';
FALSE      : 'FALSE' | 'false';
GT         : '>';
GE         : '>=';
LT         : '<';
LE         : '<=';
EQ         : '=';
MODEL      : 'clustering' | 'top-k' | 'bottom-k' | 'outliers' | 'skyline';
DECIMAL    : '-'? [0-9]+ '.' [0-9]+;
INT        : '-'? [0-9]+;
ID
  : '\'' [a-zA-Z0-9'_'\-'/' ]+ '\'' 
  |      [a-zA-Z0-9'_'\-'/']+ ;
WS         : [ \t\r\n]+ -> skip;