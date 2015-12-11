package ch.psi.bsread.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import ch.psi.bsread.message.commands.MainHeaderCommand;
import ch.psi.bsread.message.commands.ReconnectCommand;
import ch.psi.bsread.message.commands.StopCommand;

@JsonTypeInfo(
		use = JsonTypeInfo.Id.NAME,
		include = JsonTypeInfo.As.PROPERTY,
		property = "htype")
@JsonSubTypes({
		@Type(value = MainHeaderCommand.class, name = MainHeaderCommand.DEFAULT_HTYPE),
		@Type(value = ReconnectCommand.class, name = ReconnectCommand.DEFAULT_HTYPE),
		@Type(value = StopCommand.class, name = StopCommand.DEFAULT_HTYPE)
})
// see: http://stackoverflow.com/questions/24631923/alternative-to-jackson-jsonsubtypes
public abstract class PolymorphicCommandMixIn {
}
