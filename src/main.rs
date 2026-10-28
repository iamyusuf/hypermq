struct S<'a, 'b> {
    x: &'a str,
    y: &'b str,
}

struct Person {
    name: String,
    age: u32,
}

fn main() {
    let s = S { x: "hello", y: "world" };
    println!("x: {}, y: {}", s.x, s.y);


    let person = Person {
        name: String::from("Alice"),
        age: 30,
    };

    println!("Name: {}, Age: {}", person.name, person.age);
}
