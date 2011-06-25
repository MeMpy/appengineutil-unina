package it.unina.tools.datastore;

public class OpAndValue {

		  private final String operator;
		  private final Object value;

		  public OpAndValue(String operator, Object value) {
		    this.operator = operator;
		    this.value = value;
		  }

		 
		  public String getOperator() {
			return operator;
		}


		public Object getValue() {
			return value;
		}


		@Override
		  public boolean equals(Object o) {
		    if (o == null) return false;
		    if (!(o instanceof OpAndValue)) return false;
		    OpAndValue opval = (OpAndValue) o;
		    return this.operator.equals(opval.getOperator()) &&
		           this.value.equals(opval.getValue());
		  }

		}
