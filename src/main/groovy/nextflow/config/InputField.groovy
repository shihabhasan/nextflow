package nextflow.config

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

/**
 * Models a workflow input field parameter rendered in workflow webui
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@EqualsAndHashCode
@CompileStatic
class InputField {

    /**
     * The field name
     */
    String name

    /**
     * Descriptive name
     */
    String label

    /**
     * Descriptive text
     */
    String hint

    /**
     * Field type
     */
    String type


    /**
     * default value to show when the field is rendered and
     * the actual value entered by the user when submit
     */
    def value

    /**
     * Creates an empty field
     */
    InputField() { }

    /**
     * Creates a field and set the value attribute
     *
     * @param value
     */
    InputField( def value ) {
        this.value = value
    }

    /**
     * Creates a field and assign the attributes specified as a map object
     *
     * @param opts
     */
    InputField( Map opts ) {
        if( opts.containsKey('name') )
            this.name = opts.name

        if( opts.containsKey('label') )
            this.label = opts.label

        if( opts.containsKey('hint'))
            this.hint = opts.hint

        if( opts.containsKey('type') )
            this.type = opts.type

        if( opts.containsKey('value') )
            this.value = opts.value
    }

    /**
     * Creates a field and assigned the attributes specified
     *
     * @param opts
     * @param value
     */
    InputField( Map opts, def value )  {
        this(opts)
        this.value = value
    }

}
